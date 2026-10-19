package scanner

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	"github.com/Stone-afk/chain-indexer-service/internal/domain"
)

// recoverReorg 从旧 checkpoint 的前一高度开始向后查找本地链与节点当前主链的公共祖先，
// 找到后原子作废旧分支数据并把 checkpoint 回退到公共祖先。
func (s *Scanner) recoverReorg(ctx context.Context, old domain.Checkpoint, latest int64) (domain.Checkpoint, error) {
	// checkpoint 所在高度已确认发生分叉，因此从它的父高度开始在配置允许区间内逐块回溯。
	lowerBound := s.startBlock
	if candidate := old.LastProcessedHeight - s.maxReorgDepth; candidate > lowerBound {
		lowerBound = candidate
	}
	for height := old.LastProcessedHeight - 1; height >= lowerBound; height-- {
		// 读取本地在当前高度保存的 canonical 区块，作为旧分支身份。
		local, found, err := s.repo.GetCanonicalBlock(ctx, s.chainID, height)
		// 本地 canonical 区块查询失败时无法继续可靠地寻找公共祖先。
		if err != nil {
			// 补充失败高度并保留底层仓储错误链。
			return domain.Checkpoint{}, fmt.Errorf("read canonical block %d: %w", height, err)
		}
		// 公共祖先搜索要求本地每个回溯高度都有且仅有当前链对应的 canonical 区块。
		if !found || local.ChainID != s.chainID || local.BlockNumber != height {
			// 本地 canonical 历史缺失或身份错配时 fail closed，不猜测更早的祖先。
			return domain.Checkpoint{}, fmt.Errorf("canonical block missing or invalid at height %d", height)
		}
		// 按相同高度读取 RPC 节点当前认可的主链区块头。
		remote, err := s.reader.HeaderByNumber(ctx, height)
		// 节点区块头读取失败时终止恢复，避免基于不完整观察修改数据库。
		if err != nil {
			// 标明祖先候选高度并保留底层 RPC 错误。
			return domain.Checkpoint{}, fmt.Errorf("read ancestor header %d: %w", height, err)
		}
		// 防止 RPC 返回的区块高度与请求的祖先候选高度不一致。
		if remote.Number != height {
			// 高度身份异常时拒绝继续比较区块哈希。
			return domain.Checkpoint{}, fmt.Errorf("ancestor header height mismatch at height %d", height)
		}
		// 同高度哈希不同表示本地区块仍属于已经被替换的旧分支。
		if local.BlockHash != remote.Hash {
			// 继续向前回溯，直到找到本地旧分支与节点当前主链共有的区块。
			continue
		}
		// 首个哈希一致的高度就是本次重组的最近公共祖先。
		// 读取触发本轮扫描时观察到的最新高度对应区块头，记录新分支头身份。
		head, err := s.reader.HeaderByNumber(ctx, latest)
		// 新分支头读取失败时不能构造完整、可审计的重组记录。
		if err != nil {
			// 保留最新区块头读取的底层 RPC 错误。
			return domain.Checkpoint{}, fmt.Errorf("read reorg latest header: %w", err)
		}
		// 确认 RPC 返回的新分支头高度就是调用方传入的 latest。
		if head.Number != latest {
			// 最新高度身份不一致时拒绝应用重组。
			return domain.Checkpoint{}, fmt.Errorf("reorg latest header height mismatch at height %d", latest)
		}
		// 分配 16 字节随机值，用于生成本次重组的唯一 fencing 身份。
		var id [16]byte
		// 使用加密安全随机源填充全部字节。
		if _, err := rand.Read(id[:]); err != nil {
			// 无法生成重组 ID 时不得修改任何链数据。
			return domain.Checkpoint{}, fmt.Errorf("generate reorg ID: %w", err)
		}
		// 设置 UUID version 4 位，表明该 ID 来自随机生成。
		id[6] = (id[6] & 0x0f) | 0x40
		// 设置 RFC 4122 variant 位，形成标准 UUID 字节布局。
		id[8] = (id[8] & 0x3f) | 0x80
		// 汇总公共祖先、旧 checkpoint 和新链头，形成完整重组事实。
		reorg := domain.Reorg{
			// 将随机字节格式化为带连字符的 UUID 字符串。
			ReorgID: fmt.Sprintf("%x-%x-%x-%x-%x", id[:4], id[4:6], id[6:8], id[8:10], id[10:]),
			// 记录发生重组的链，以及已经由哈希比较确认的公共祖先。
			ChainID: s.chainID, AncestorHeight: height, AncestorHash: remote.Hash,
			// 记录重组前已持久化 checkpoint 指向的旧分支头。
			OldHeadHeight: old.LastProcessedHeight, OldHeadHash: old.LastProcessedHash,
			// 记录本轮扫描观察到的新分支最新区块头。
			NewHeadHeight: latest, NewHeadHash: head.Hash,
			// 深度表示旧 checkpoint 比公共祖先高出的区块数量，同时记录检测时间。
			ReorgDepth: int(old.LastProcessedHeight - height), DetectedAt: time.Now().Unix(),
		}
		// 构造重组完成后应保存的新 checkpoint，使后续扫描从祖先下一高度重新开始。
		ancestor := domain.Checkpoint{ChainID: s.chainID, LastProcessedHeight: height, LastProcessedHash: remote.Hash}
		// 在 repository 的同一事务内记录重组、作废旧分支数据并回退 checkpoint。
		if err := s.repo.ApplyReorg(ctx, reorg, ancestor); err != nil {
			// 应用失败时返回错误，数据库事务负责保证不留下部分重组状态。
			return domain.Checkpoint{}, fmt.Errorf("apply reorg: %w", err)
		}
		// 返回公共祖先 checkpoint，调用方会从其下一高度重新扫描当前 canonical 分支。
		return ancestor, nil
	}
	return domain.Checkpoint{}, fmt.Errorf("automatic reorg recovery boundary exhausted: chainID=%d oldHeight=%d maxDepth=%d lowerBound=%d: %w", s.chainID, old.LastProcessedHeight, s.maxReorgDepth, lowerBound, ErrMaxReorgDepthExceeded)
}
