package ioc

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"testing"
)

func TestRequiredSchemaVersionIsThree(t *testing.T) {
	path := filepath.Join("db.go")
	f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	initDB := false
	checkCalls := 0
	ast.Inspect(f, func(n ast.Node) bool {
		fn, ok := n.(*ast.FuncDecl)
		if ok {
			if fn.Name.Name != "InitDB" {
				return false
			}
			initDB = true
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				id, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || id.Sel.Name != "CheckSchemaVersion" {
					return true
				}
				recv, ok := id.X.(*ast.Ident)
				if !ok || recv.Name != "postgre" {
					t.Fatalf("CheckSchemaVersion receiver must be postgre")
				}
				checkCalls++
				if len(call.Args) != 3 {
					t.Fatalf("CheckSchemaVersion argument count=%d", len(call.Args))
				}
				lit, ok := call.Args[2].(*ast.BasicLit)
				if !ok || lit.Value != "3" {
					t.Errorf("CheckSchemaVersion version must be literal 3, got %#v", call.Args[2])
				}
				found = true
				return true
			})
			return false
		}
		return true
	})
	if !found {
		t.Fatal("InitDB must call CheckSchemaVersion")
	}
	if !initDB || checkCalls != 1 {
		t.Fatalf("InitDB=%v CheckSchemaVersion calls=%d", initDB, checkCalls)
	}
}
