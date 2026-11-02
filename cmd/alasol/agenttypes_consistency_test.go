package main

// 跨包不变量（T5-1）：cmd/alasol 的**内建 agent 类型副本**与 host 的那份**逐字节一致**。
//
// # 为什么需要这条钉子
//
// 内建五类（planner / coder / critic / reviewer / observer）有**两份**实现：
//
//   - host/agenttypes.go 的 builtinAgentTypes() —— **serve**（host 装配）走它；
//   - cmd/alasol/multiagent.go 的 builtinAgentTypes() —— **run / resume** 走它（host 的
//     那份未导出，而范围铁律不许为此改 host）。
//
// 两份靠"人工同步"维持 = 迟早漂移：措辞 / 工具白名单 / 权限档 / can_spawn 任一处不同，
// 同一份 TOML 在 serve 与 run 下派出来的子 agent 就不是一回事（用户看不出差别，只会觉得
// "有时子 agent 会读文件，有时不会"）。故要有**跨包不变量**钉住。
//
// # 怎么比（host 没导出 → 比源码）
//
// host 的 builtinAgentTypes 是**未导出**函数（cmd 侧拿不到它，也没有"导出后比对"的
// 公开面）→ 本测试**解析 host/agenttypes.go 的源码**（go/parser），取出该函数返回的
// 字面量，规范化成与 cmd 侧**同一形状**的结构；两边各 dump 一份 JSON，**逐字节**比对。
//
// 规范化只做"字面量求值"（字符串字面量 / 同文件常量 / contracts 的权限档常量）——
// 不比"运行时行为"，比的是**两份字面量本身**（这正是会漂移的东西）。
//
// # 变异自证
//
// 把 host/agenttypes.go 的内建五类改一处（措辞 / tools / permission / can_spawn）→
// 本用例红（两份不再逐字节一致）。

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
)

// canonicalAgentType 是内建 agent 类型的**规范化形状**（两份字面量都折成它再比）。
//
// 字段与 contracts.AgentType 的相关字段一一对应；空切片统一成 nil（避免
// `[]` vs `null` 的 JSON 差异）。
type canonicalAgentType struct {
	ID       string         `json:"id"`
	Prompt   string         `json:"prompt"`
	Tools    []string       `json:"tools"`
	Model    string         `json:"model"`
	CanSpawn []string       `json:"can_spawn"`
	Perm     *canonicalPerm `json:"permission_request,omitempty"`
	// 补齐 8 字段中的后两个（critic F1：此前永不比对 = 漂移盲区）：
	ContextStrategy    string `json:"context_strategy_kind,omitempty"` // 空 = 未声明
	ShareProjectAssets *bool  `json:"share_project_assets,omitempty"`
}

// canonicalPerm 是 permission_request 的规范化形状（read/write/exec 三档）。
type canonicalPerm struct {
	Read  string `json:"read"`
	Write string `json:"write"`
	Exec  string `json:"exec"`
}

// contractLevelValues 是 contracts 里权限档常量的**取值表**（源侧字面量求值用）。
//
// 用 map 而不是反射：contracts 的这几档是**常量**（编译期已知），显式列出来 = 编译期
// 可核对（常量改名会编译失败），不会静默漂移。
var contractLevelValues = map[string]string{
	"ReadWriteLevelNone":      string(contracts.ReadWriteLevelNone),
	"ReadWriteLevelWorkspace": string(contracts.ReadWriteLevelWorkspace),
	"ReadWriteLevelFull":      string(contracts.ReadWriteLevelFull),
	"ExecLevelNone":           string(contracts.ExecLevelNone),
	"ExecLevelAllowlist":      string(contracts.ExecLevelAllowlist),
	"ExecLevelFull":           string(contracts.ExecLevelFull),
}

// canonicalFromContracts 把**已编译**的 contracts.AgentType 折成规范化形状（cmd 侧用）。
func canonicalFromContracts(t contracts.AgentType) canonicalAgentType {
	out := canonicalAgentType{
		ID:       t.ID,
		Prompt:   t.Prompt,
		Tools:    t.Tools,
		Model:    t.Model,
		CanSpawn: t.CanSpawn,
	}
	if t.PermissionRequest != nil {
		out.Perm = &canonicalPerm{
			Read:  string(t.PermissionRequest.Read),
			Write: string(t.PermissionRequest.Write),
			Exec:  string(t.PermissionRequest.Exec),
		}
	}
	if t.ContextStrategy != nil {
		out.ContextStrategy = string(t.ContextStrategy.Kind)
	}
	if t.ShareProjectAssets != nil {
		v := *t.ShareProjectAssets
		out.ShareProjectAssets = &v
	}
	return out
}

// TestBuiltinAgentTypesMatchHostByteForByte（T5-1 跨包不变量）：cmd/alasol 的内建类型
// 副本与 host/agenttypes.go 的那份**逐字节一致**（JSON dump 相同）。
//
// 拿不到 host 的导出面（未导出 + 范围铁律不许改 host）→ 比**源码字面量**
// （go/parser 求值）。host 的源文件不在预期位置 → skip 并说明（不静默通过）。
func TestBuiltinAgentTypesMatchHostByteForByte(t *testing.T) {
	hostPath := hostAgentTypesSourcePath(t)
	hostTypes, err := parseHostBuiltinAgentTypes(hostPath)
	if err != nil {
		// critic F1：解析失败**不许 skip**（静默绿 = 漂移盲区）——仓内字面量形状变了就该
		// 响亮失败，来人扩本解析器。
		t.Fatalf("解析 host 的内建类型副本失败（%s）：%v", hostPath, err)
	}

	cmdTypes := make([]canonicalAgentType, 0, 5)
	for _, at := range builtinAgentTypes() {
		cmdTypes = append(cmdTypes, canonicalFromContracts(at))
	}

	hostJSON, err := json.Marshal(hostTypes)
	if err != nil {
		t.Fatalf("序列化 host 侧: %v", err)
	}
	cmdJSON, err := json.Marshal(cmdTypes)
	if err != nil {
		t.Fatalf("序列化 cmd 侧: %v", err)
	}
	if string(hostJSON) != string(cmdJSON) {
		t.Fatalf("cmd/alasol 与 host 的内建 agent 类型副本**不一致**（两份漂移了）：\nhost = %s\ncmd  = %s",
			hostJSON, cmdJSON)
	}
}

// hostAgentTypesSourcePath 定位 host/agenttypes.go（相对本测试文件 → 与 CWD 无关）。
func hostAgentTypesSourcePath(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Skip("拿不到本文件路径（runtime.Caller 失败），跳过跨包不变量")
	}
	pkgDir := filepath.Dir(thisFile)
	// 本文件在 <repo>/cmd/alasol/ → host 的源文件在 <repo>/host/（上两级）。
	p := filepath.Join(pkgDir, "..", "..", "host", "agenttypes.go")
	if _, err := os.Stat(p); err != nil {
		// critic F1：仓内文件必存在——找不到 = 仓库结构变了，响亮失败而不是静默跳过。
		t.Fatalf("找不到 host 的内建类型源文件 %s：%v", p, err)
	}
	return p
}

// parseHostBuiltinAgentTypes 解析 host/agenttypes.go，取出 builtinAgentTypes 返回的
// 字面量并求值成规范化形状。
func parseHostBuiltinAgentTypes(path string) ([]canonicalAgentType, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		return nil, err
	}
	// 同文件的字符串常量（host 的 builtinAgentTypes 用 agentTypePlanner 这类常量当 id）。
	consts := map[string]string{}
	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.CONST {
			continue
		}
		for _, spec := range gd.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for i, name := range vs.Names {
				if i >= len(vs.Values) {
					continue
				}
				if s, ok := evalHostString(vs.Values[i], consts); ok {
					consts[name.Name] = s
				}
			}
		}
	}
	// 找 builtinAgentTypes 函数。
	var fn *ast.FuncDecl
	for _, decl := range f.Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if ok && fd.Name.Name == "builtinAgentTypes" {
			fn = fd
			break
		}
	}
	if fn == nil {
		return nil, errNoBuiltinFunc
	}
	// 取它的返回值（一个 []contracts.AgentType 字面量）。
	var lit *ast.CompositeLit
	ast.Inspect(fn, func(n ast.Node) bool {
		if lit != nil {
			return false
		}
		if cl, ok := n.(*ast.CompositeLit); ok {
			lit = cl
			return false
		}
		return true
	})
	if lit == nil {
		return nil, errNoBuiltinFunc
	}
	out := make([]canonicalAgentType, 0, len(lit.Elts))
	for _, el := range lit.Elts {
		cl, ok := el.(*ast.CompositeLit)
		if !ok {
			return nil, errUnexpectedLiteral
		}
		ct, err := evalHostAgentType(cl, consts)
		if err != nil {
			return nil, err
		}
		out = append(out, ct)
	}
	return out, nil
}

// evalHostAgentType 求值一个 contracts.AgentType 字面量（键值字段）。
func evalHostAgentType(cl *ast.CompositeLit, consts map[string]string) (canonicalAgentType, error) {
	var out canonicalAgentType
	for _, elt := range cl.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			return out, errUnexpectedLiteral
		}
		key, ok := kv.Key.(*ast.Ident)
		if !ok {
			return out, errUnexpectedLiteral
		}
		switch key.Name {
		case "ID":
			s, ok := evalHostString(kv.Value, consts)
			if !ok {
				return out, errUnexpectedLiteral
			}
			out.ID = s
		case "Prompt":
			s, ok := evalHostString(kv.Value, consts)
			if !ok {
				return out, errUnexpectedLiteral
			}
			out.Prompt = s
		case "Model":
			s, ok := evalHostString(kv.Value, consts)
			if !ok {
				return out, errUnexpectedLiteral
			}
			out.Model = s
		case "Tools":
			list, ok := evalHostStringList(kv.Value, consts)
			if !ok {
				return out, errUnexpectedLiteral
			}
			out.Tools = list
		case "CanSpawn":
			// nil（叶子类型）与 []string{...} 两种写法都要认。
			if id, ok := kv.Value.(*ast.Ident); ok && id.Name == "nil" {
				out.CanSpawn = nil
				continue
			}
			list, ok := evalHostStringList(kv.Value, consts)
			if !ok {
				return out, errUnexpectedLiteral
			}
			out.CanSpawn = list
		case "PermissionRequest":
			perm, ok := evalHostPermission(kv.Value, consts)
			if !ok {
				return out, errUnexpectedLiteral
			}
			out.Perm = perm
		case "ContextStrategy":
			// nil（未声明）与 &contracts.ContextStrategy{Kind: …} 两种形态；认不出就响亮失败。
			if id, ok := kv.Value.(*ast.Ident); ok && id.Name == "nil" {
				out.ContextStrategy = ""
				continue
			}
			kind, ok := evalHostContextStrategyKind(kv.Value, consts)
			if !ok {
				return out, errUnexpectedLiteral
			}
			out.ContextStrategy = kind
		case "ShareProjectAssets":
			// nil 与 &bool 形态（&true 不合法 Go，实际是取地址表达式——认不出就响亮失败）。
			if id, ok := kv.Value.(*ast.Ident); ok && id.Name == "nil" {
				out.ShareProjectAssets = nil
				continue
			}
			b, ok := evalHostBoolPtr(kv.Value, consts)
			if !ok {
				return out, errUnexpectedLiteral
			}
			out.ShareProjectAssets = b
		default:
			// 本副本只用到上面几个字段；出现别的字段 → 保守报错（别默默忽略差异）。
			return out, errUnexpectedLiteral
		}
	}
	return out, nil
}

// evalHostContextStrategyKind 求值 &contracts.ContextStrategy{Kind: …} 字面量（取 Kind）。
func evalHostContextStrategyKind(expr ast.Expr, consts map[string]string) (string, bool) {
	if u, ok := expr.(*ast.UnaryExpr); ok && u.Op == token.AND {
		expr = u.X
	}
	cl, ok := expr.(*ast.CompositeLit)
	if !ok {
		return "", false
	}
	for _, elt := range cl.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			return "", false
		}
		key, ok := kv.Key.(*ast.Ident)
		if !ok || key.Name != "Kind" {
			return "", false
		}
		return evalHostString(kv.Value, consts)
	}
	return "", false
}

// evalHostBoolPtr 求值指向布尔字面量的取地址表达式（&true 不合法 Go，实际形态是
// 取地址复合/调用——认不出就返回 false，让上层响亮失败而不是静默忽略）。
func evalHostBoolPtr(expr ast.Expr, consts map[string]string) (*bool, bool) {
	// 形态一：取地址一个布尔标识符（true/false）。
	if u, ok := expr.(*ast.UnaryExpr); ok && u.Op == token.AND {
		if id, ok := u.X.(*ast.Ident); ok {
			switch id.Name {
			case "true":
				v := true
				return &v, true
			case "false":
				v := false
				return &v, true
			}
		}
	}
	// 形态二：形如 &[]bool{true}[0] 的复合取元素（只认字面量元素）。
	if u, ok := expr.(*ast.UnaryExpr); ok && u.Op == token.AND {
		if ix, ok := u.X.(*ast.IndexExpr); ok {
			if cl, ok := ix.X.(*ast.CompositeLit); ok && len(cl.Elts) == 1 {
				if id, ok := cl.Elts[0].(*ast.Ident); ok {
					switch id.Name {
					case "true":
						v := true
						return &v, true
					case "false":
						v := false
						return &v, true
					}
				}
			}
		}
	}
	return nil, false
}

// evalHostPermission 求值 &contracts.PermissionRequest{...} 字面量。
func evalHostPermission(expr ast.Expr, consts map[string]string) (*canonicalPerm, bool) {
	if u, ok := expr.(*ast.UnaryExpr); ok && u.Op == token.AND {
		expr = u.X
	}
	cl, ok := expr.(*ast.CompositeLit)
	if !ok {
		return nil, false
	}
	out := &canonicalPerm{}
	for _, elt := range cl.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			return nil, false
		}
		key, ok := kv.Key.(*ast.Ident)
		if !ok {
			return nil, false
		}
		val, ok := evalHostString(kv.Value, consts)
		if !ok {
			return nil, false
		}
		switch key.Name {
		case "Read":
			out.Read = val
		case "Write":
			out.Write = val
		case "Exec":
			out.Exec = val
		default:
			return nil, false
		}
	}
	return out, true
}

// evalHostStringList 求值 []string{...} 字面量。
func evalHostStringList(expr ast.Expr, consts map[string]string) ([]string, bool) {
	cl, ok := expr.(*ast.CompositeLit)
	if !ok {
		return nil, false
	}
	out := make([]string, 0, len(cl.Elts))
	for _, el := range cl.Elts {
		s, ok := evalHostString(el, consts)
		if !ok {
			return nil, false
		}
		out = append(out, s)
	}
	return out, true
}

// evalHostString 求值一个字面量：字符串字面量 / 同文件常量 / contracts 的权限档常量。
// contextStrategyKindConsts 是 contracts 的 ContextStrategyKind 常量族闭合映射
// （与 contracts/workflow.go 的枚举表同源；新增 kind 要同步补这里，否则解析响亮失败）。
var contextStrategyKindConsts = map[string]string{
	"ContextStrategyKindAppend":        "append",
	"ContextStrategyKindStageScoped":   "stage_scoped",
	"ContextStrategyKindSlidingWindow": "sliding_window",
	"ContextStrategyKindTiered":        "tiered",
}

func evalHostString(expr ast.Expr, consts map[string]string) (string, bool) {
	switch e := expr.(type) {
	case *ast.BasicLit:
		if e.Kind != token.STRING {
			return "", false
		}
		s, err := strconv.Unquote(e.Value)
		if err != nil {
			return "", false
		}
		return s, true
	case *ast.Ident:
		if s, ok := consts[e.Name]; ok {
			return s, true
		}
		return "", false
	case *ast.SelectorExpr:
		// contracts.ReadWriteLevelWorkspace 这类：按**选择子名**查取值表；
		// ContextStrategyKind 常量族同理（闭合映射，对齐 contracts/workflow.go）。
		if s, ok := contractLevelValues[e.Sel.Name]; ok {
			return s, true
		}
		if s, ok := contextStrategyKindConsts[e.Sel.Name]; ok {
			return s, true
		}
		return "", false
	default:
		return "", false
	}
}

// errNoBuiltinFunc / errUnexpectedLiteral 是解析失败的原因（测试据此 skip 并说明）。
var (
	errNoBuiltinFunc     = &parseErr{"找不到 builtinAgentTypes 函数（host 的实现形状变了？）"}
	errUnexpectedLiteral = &parseErr{"遇到不认识的字面量形状（host 的实现形状变了？）"}
)

// parseErr 是解析失败的错误类型。
type parseErr struct{ msg string }

func (e *parseErr) Error() string { return e.msg }

// 便于排查：把常量值的名字也打出来（失败信息里能看到是哪一档对不上）。
var _ = strings.TrimSpace
