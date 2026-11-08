"""MCP server (stdio) bọc tool của agent: hỏi FinGraph trực tiếp từ Claude Desktop / Claude Code khi demo
(bước A8 giai đoạn 2, docs/AI-AGENT.md mục 10).

    python -m agent.mcp_server --workgroup fingraph-dev-analyst [--neptune fingraph-dev-neptune-ops]

Khác với agent/loop.py: ở đây VÒNG LẶP do ứng dụng host (Claude Desktop / Claude Code) chạy, nên kiểm
grounding không chen vào được giữa các bước. Bù lại:
  - `instructions` của server = đúng quy tắc + catalog của agent (RULES, catalog rút gọn);
  - thêm tool `check_citations(answer)`: host gọi trước khi trả lời, đối chiếu id với kết quả tool của phiên
    (cùng hàm grounding.check). Đây là kiểm tự nguyện, KHÔNG thay được kiểm bắt buộc của loop.py; vì vậy
    bộ đánh giá (agent/eval/run.py) luôn chạy qua loop.py.
Mọi tool, kiểm tham số, quyền persona (Athena workgroup, Lake Formation) giữ nguyên như loop.py.

Cấu hình Claude Code (một lần):
    claude mcp add fingraph -- <đường dẫn>\\.venv\\Scripts\\python.exe -m agent.mcp_server --workgroup fingraph-dev-analyst
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict, List

from . import grounding as G
from .loop import system_blocks
from .tools import TOOL_SPECS, AthenaBackend, NeptuneBackend, Tools

CHECK_SPEC = {
    "name": "check_citations",
    "description": "Kiem trich dan TRUOC KHI tra loi nguoi dung: moi [uid:...] / [triple:...] trong cau tra loi phai "
                   "co trong ket qua tool cua phien nay; cau tra loi neu du lieu ma khong trich id nao cung bi chan. "
                   "ok=false thi sua cau tra loi (hoac goi them tool) roi kiem lai.",
    "input_schema": {"type": "object", "properties": {"answer": {"type": "string"}}, "required": ["answer"],
                     "additionalProperties": False},
}


def tool_list() -> List[Dict[str, Any]]:
    """Định nghĩa tool cho MCP: bỏ `strict` (khoá riêng của Messages API), giữ nguyên schema."""
    return [{k: v for k, v in s.items() if k != "strict"} for s in TOOL_SPECS] + [CHECK_SPEC]


def handle(tools: Tools, name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    """Một lời gọi tool -> (payload JSON, is_error). Tách khỏi phần MCP để test không cần thư viện mcp."""
    if name == "check_citations":
        rep = G.check(str(args.get("answer", "")), tools.seen_ids)
        return {"ok": rep.ok, "reason": rep.reason, "unknown_ids": rep.unknown, "cited": len(rep.cited)}
    return tools.call(name, args)


def build_server(tools: Tools):
    import mcp_types as types
    from mcp.server.lowlevel import Server

    specs = tool_list()

    async def on_list_tools(ctx, params):
        return types.ListToolsResult(tools=[types.Tool(name=s["name"], description=s["description"],
                                                       input_schema=s["input_schema"]) for s in specs])

    async def on_call_tool(ctx, params):
        out = handle(tools, params.name, dict(params.arguments or {}))
        is_err = bool(out.pop("_is_error", False))
        return types.CallToolResult(content=[types.TextContent(type="text", text=json.dumps(
            out, ensure_ascii=False, default=str))], is_error=is_err)

    return Server("fingraph", version="0.1", instructions=system_blocks(tools.cat)[0]["text"],
                  on_list_tools=on_list_tools, on_call_tool=on_call_tool)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="MCP server FinGraph (stdio)")
    ap.add_argument("--workgroup", required=True)
    ap.add_argument("--region", default="ap-southeast-1")
    ap.add_argument("--neptune", default=None)
    a = ap.parse_args(argv)

    import anyio
    from mcp.server.stdio import stdio_server

    # stdout là kênh giao thức MCP: log tool phải ra stderr
    tools = Tools(AthenaBackend(a.workgroup, region=a.region),
                  graph=NeptuneBackend(a.neptune, region=a.region) if a.neptune else None,
                  log=lambda r: print(f"[tool] {json.dumps(r, ensure_ascii=False)}", file=sys.stderr))
    server = build_server(tools)

    async def serve():
        async with stdio_server() as (read, write):
            await server.run(read, write, server.create_initialization_options())

    anyio.run(serve)
    return 0


if __name__ == "__main__":
    sys.exit(main())
