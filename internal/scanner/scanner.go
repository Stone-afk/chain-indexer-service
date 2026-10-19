package scanner

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Stone-afk/chain-indexer-service/internal/domain"
)

var (
	ErrFinalIdentityMismatch = errors.New("final block identity mismatch")
	ErrStateFenceRejected    = errors.New("stored block state fence rejected")
)

func (s *Scanner) HandleNewHead(ctx context.Context) error {
	// 读取节点当前已知的最新区块高度，作为本轮扫描范围的上界来源。
	latest, err := s.reader.LatestBlockNumber(ctx)
	// 节点高度读取失败时立即返回，避免基于未知高度继续扫描。
	if err != nil {
		// 包装底层错误，保留 errors.Is/errors.As 错误链。
		return fmt.Errorf("read latest block number: %w", err)
	}
	// 最新高度尚未达到确认块数时，没有可安全处理的确认区块。
	if latest < s.confirmBlocks {
		// 本轮无需处理任何区块，正常返回。
		return nil
	}
	// 只扫描已达到确认深度的高度，暂不处理链尖附近的区块。
	safe := latest - s.confirmBlocks
	// 读取当前链的持久化扫描进度，用于决定首次扫描或断点续扫。
	cp, found, err := s.repo.GetCheckpoint(ctx, s.chainID)
	// checkpoint 查询失败时不能可靠确定下一扫描高度。
	if err != nil {
		// 返回带操作上下文的底层仓储错误。
		return fmt.Errorf("read checkpoint: %w", err)
	}
	// 默认从配置中显式指定的 startBlock 开始扫描。
	next := s.startBlock
	// previousHash 保存前一个已确认区块哈希，用于验证父子链连续性。
	previousHash := ""
	// 已存在 checkpoint 时，先确认它仍位于节点当前主链上。
	if found {
		// 按 checkpoint 高度重新读取节点当前认可的区块头。
		header, err := s.reader.HeaderByNumber(ctx, cp.LastProcessedHeight)
		// 无法读取 checkpoint 对应区块头时停止本轮扫描。
		if err != nil {
			// 保留底层 RPC 错误，补充 checkpoint 读取语义。
			return fmt.Errorf("read checkpoint header: %w", err)
		}
		// 防止 RPC 返回的区块高度与请求的 checkpoint 高度不一致。
		if header.Number != cp.LastProcessedHeight {
			// 高度身份不一致时拒绝继续推进 checkpoint。
			return fmt.Errorf("checkpoint header height mismatch at height %d", cp.LastProcessedHeight)
		}
		// 同高度哈希变化表示 checkpoint 所在分支已经发生重组。
		if header.Hash != cp.LastProcessedHash {
			// 查找公共祖先、作废旧分支数据，并返回恢复后的 checkpoint。
			cp, err = s.recoverReorg(ctx, cp, latest)
			// 重组恢复失败时保持错误语义并终止扫描。
			if err != nil {
				// recoverReorg 已提供完整上下文，这里直接透传。
				return err
			}
		}
		// 安全高度没有超过 checkpoint 时，本轮没有新区块需要处理。
		if safe <= cp.LastProcessedHeight {
			// checkpoint 已追平当前安全高度，正常返回。
			return nil
		}
		// 断点续扫从 checkpoint 的下一个高度开始。
		next = cp.LastProcessedHeight + 1
		// 使用 checkpoint 哈希校验下一块的 parentHash。
		previousHash = cp.LastProcessedHash
	}
	// 按高度串行扫描 next 到 safe 的所有已确认区块。
	for height := next; height <= safe; height++ {
		// 获取包含完整交易列表的目标区块。
		block, err := s.reader.BlockByNumber(ctx, height)
		// 区块读取失败时停止，不能越过失败高度推进 checkpoint。
		if err != nil {
			// 在错误中记录具体失败高度并保留底层原因。
			return fmt.Errorf("read block %d: %w", height, err)
		}
		// 校验返回区块的高度与当前循环高度完全一致。
		if block.Header.Number != height {
			// 区块身份异常时拒绝映射和保存。
			return fmt.Errorf("block height mismatch at height %d", height)
		}
		// 续扫或批内后续区块必须与前一已确认区块保持父子关系。
		if (found || height > next) && block.Header.ParentHash != previousHash {
			// 父哈希不连续时停止，避免把不同分支拼接到同一 canonical 链。
			return fmt.Errorf("block parent hash mismatch at height %d", height)
		}
		// 为区块内每笔交易预分配 receipt 容器，保持交易顺序。
		receipts := make([]domain.Receipt, 0, len(block.Transactions))
		// 串行读取区块内每笔交易的执行回执。
		for _, tx := range block.Transactions {
			// 使用交易哈希查询对应 receipt。
			receipt, err := s.reader.ReceiptByTransactionHash(ctx, tx.Hash)
			// 任一 receipt 缺失都会使整个区块数据不完整。
			if err != nil {
				// 返回失败区块高度及底层 RPC 错误。
				return fmt.Errorf("read receipt at block %d: %w", height, err)
			}
			// 按交易顺序收集 receipt，供 mapper 做身份和执行结果映射。
			receipts = append(receipts, receipt)
		}
		// 将区块、交易和回执转换成一次原子保存所需的 canonical batch。
		batch, err := mapCanonicalBatch(s.chainID, block, receipts, time.Now().Unix())
		// 映射失败说明链数据身份或字段不满足持久化契约。
		if err != nil {
			// 补充区块高度后返回 mapper 的具体错误。
			return fmt.Errorf("map block %d: %w", height, err)
		}
		// 按区块哈希查询本地已有状态，区分新增、重复验证和显式恢复。
		stored, found, err := s.repo.GetBlockByHash(ctx, s.chainID, block.Header.Hash)
		// 本地状态分类失败时不能安全选择写入路径。
		if err != nil {
			// 返回分类阶段和区块高度，并保留仓储错误链。
			return fmt.Errorf("classify block %d: %w", height, err)
		}
		// 落库前再次读取同高度区块头，缩小首次读取到数据库提交之间的重组窗口。
		// 该复核无法消除 RPC 读取后到数据库提交前的窗口，因此不是分布式一致性屏障。
		finalHeader, err := s.reader.HeaderByNumber(ctx, height)
		// 最终区块头读取失败时不执行任何保存或恢复操作。
		if err != nil {
			// 标记这是落库前的最终区块头读取失败。
			return fmt.Errorf("read final block header %d: %w", height, err)
		}
		// 最终观察的高度、哈希和父哈希必须与已完成映射的区块完全一致。
		if finalHeader.Number != block.Header.Number || finalHeader.Hash != block.Header.Hash || finalHeader.ParentHash != block.Header.ParentHash {
			// 使用稳定 sentinel 标记最终 canonical 身份发生变化。
			return fmt.Errorf("final block header mismatch at height %d: %w", height, ErrFinalIdentityMismatch)
		}
		// 本地不存在该区块时，走普通 canonical 新增路径。
		if !found {
			// repository 会再次校验 batch 与最终观察，并原子保存完整批次。
			if err := s.repo.SaveCanonicalBatch(ctx, batch, finalHeader); err != nil {
				// 保存失败时不推进内存中的 previousHash。
				return fmt.Errorf("save block %d: %w", height, err)
			}
		} else {
			// 本地已存在相同哈希区块时，根据持久化状态选择受控路径。
			switch stored.Status {
			// CANONICAL 表示重复消费，只允许执行严格的 validation-only replay。
			case domain.BlockStatusCanonical.ToString():
				// 普通保存会验证已有 scanner-owned 数据，不补写或恢复失效数据。
				if err := s.repo.SaveCanonicalBatch(ctx, batch, finalHeader); err != nil {
					// replay 验证失败时返回分类后的仓储错误。
					return fmt.Errorf("replay block %d: %w", height, err)
				}
			// ORPHANED 表示该区块曾因重组失效，只能走显式 fenced 恢复。
			case domain.BlockStatusOrphaned.ToString():
				// 显式恢复必须携带造成当前失效状态的原始 reorg ID。
				if stored.ReorgID == nil || *stored.ReorgID == "" {
					// 缺少 fencing 身份时拒绝恢复，避免误激活旧分支数据。
					return fmt.Errorf("orphaned block %d missing reorg ID: %w", height, ErrStateFenceRejected)
				}
				// 使用精确 reorg ID 恢复已存在且完整匹配的 orphaned 子树。
				if err := s.repo.ReactivateCanonicalBatch(ctx, batch, finalHeader, *stored.ReorgID); err != nil {
					// 恢复失败时保留 repository 的 identity/state/conflict 分类。
					return fmt.Errorf("reactivate block %d: %w", height, err)
				}
			// 任何未支持状态都必须 fail closed，不能猜测对应写入行为。
			default:
				// 使用稳定 sentinel 表明持久化状态未通过 scanner fence。
				return fmt.Errorf("unsupported stored block status %q at height %d: %w", stored.Status, height, ErrStateFenceRejected)
			}
		}
		// 当前区块成功处理后，记录其哈希供下一高度验证 parentHash。
		previousHash = block.Header.Hash
		// 到达本轮安全高度后显式结束，避免 uint64 最大值自增溢出。
		if height == safe {
			// 终止本轮串行扫描循环。
			break
		}
	}
	// 所有待处理安全区块均已成功保存、验证或显式恢复。
	return nil
}
