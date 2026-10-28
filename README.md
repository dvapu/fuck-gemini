# fuck-gemini

**Skip the Antigravity app entirely. Rotate Google accounts with OAuth refresh
tokens and call the Gemini backend directly.**

[English](#english) | [Tiếng Việt](#tiếng-việt)

---

## English

### What this is

A single-file MCP server (`agy_pool.py`, ~35 KB, no dependencies beyond
`mcp >= 2.0`) that turns a pool of Google accounts into a rotation-capable
Gemini API endpoint:

- **Direct HTTPS** to `daily-cloudcode-pa.googleapis.com` — the same backend the
  Antigravity desktop app uses.
- **No Antigravity conversations are created.** Verified: 6 requests →
  conversation count unchanged. Your Antigravity / Gemini chat history stays
  clean.
- **Automatic OAuth refresh.** Expired access tokens are refreshed with the
  account's refresh token; rotated tokens are written back atomically so
  [agy-switch](https://github.com/anglee0323/agy-switch) keeps working.
- **Round-robin rotation with cooldowns.** 429 → 5 min cooldown, 401/403 →
  15 min, timeouts/5xx → 60 s. The pool walks to the next account automatically.
- **Parallel fan-out.** `agy_pool_fanout` fires independent subtasks at several
  accounts at once and collects every answer in one call (measured: 6 requests
  in 4.7 s wall time vs ~24 s sequential — a 5x speedup for independent work).

### Do I need agy-switch installed?

**No — but it is the recommended source of accounts.**

The only hard requirement is that `~/.antigravity_tools/accounts/*.json` exists
and contains valid account files. Those files come from either:

1. **[agy-switch](https://github.com/anglee0323/agy-switch)** — recommended. A
   desktop tool that manages multiple Google accounts for Antigravity: add
   accounts, refresh tokens, rotate the active account. Its data directory is
   `~/.antigravity_tools` and this pool reads the same files. Install it, add
   your Google accounts, done.
2. **Write the JSON files yourself.** Any tool that can produce a file like:

   ```json
   {
     "id": "any-unique-id",
     "email": "you@gmail.com",
     "disabled": false,
     "token": {
       "access_token": "...",
       "refresh_token": "...",
       "expiry_timestamp": 1793176419,
       "project_id": "aicode-consumers"
     }
   }
   ```

   `access_token` + `expiry_timestamp` are enough to start; with
   `refresh_token` plus OAuth client credentials (see below) the pool can
   refresh forever on its own. Set `ABV_DATA_DIR` if your files live elsewhere.

### OAuth client credentials (needed for auto-refresh)

The pool needs a Google OAuth client id/secret to exchange refresh tokens.
Resolution order:

1. `AGY_CLIENT_ID` / `AGY_CLIENT_SECRET` environment variables
2. `%LOCALAPPDATA%/agy-pool/oauth.json` (or `~/.config/agy-pool/oauth.json`) —
   `{"client_id": "...", "client_secret": "..."}`, created by you, never committed
3. Optional fallback: a `.env` file with `AGY_CLIENT_ID=` / `AGY_CLIENT_SECRET=`

Where to get them: the Antigravity client credentials (`oauth_client_key:
antigravity_enterprise`) work, or register your own OAuth client on Google Cloud.
Without credentials the pool still works — it just cannot refresh expired tokens.

### Install (OpenCode)

```jsonc
// ~/.config/opencode/opencode.json
{
  "mcp": {
    "servers": {
      "agy-pool": {
        "type": "local",
        "command": ["python", "/full/path/to/agy_pool.py"],
        "environment": {
          "AGY_CLIENT_ID": "{env:AGY_CLIENT_ID}",
          "AGY_CLIENT_SECRET": "{env:AGY_CLIENT_SECRET}"
        }
      }
    }
  }
}
```

Works with any MCP client (Claude Desktop, etc.) — same stdio transport.

### The orchestration loop

The pool is a brain, not a pair of hands. **It cannot touch the filesystem.**
Your coding agent (OpenCode, Claude Code, anything that speaks MCP) runs the
loop:

```
agent gives task  →  pool returns JSON edits  →  agent applies + verifies
        ↑                                                 |
        └───────────── next task with failures ──────────┘
```

1. **Give a task** — `agy_pool_code_task` with goal, acceptance criteria,
   relevant file contents, constraints. Returns `changes[]`
   (create/replace/patch), `verification[]`, `risks[]`.
2. **Apply it yourself** — the agent writes the files.
3. **Verify for real** — run the tests/build. Never trust the model's `done`
   flag.
4. **Give the next task** — feed failures back, repeat until green.

### Fan-out: many agents at once (agent-as-boss)

When a problem can be split, or when thinking time is the bottleneck, use
`agy_pool_fanout`: divide the work into independent subtasks, fire them at
several Gemini agents in parallel, and collect all outputs in one call.

Two valid ways to use it:

- **Divide**: split a big change along file or module boundaries — each agent
  owns disjoint pieces, you merge afterwards. Subtasks MUST be independent;
  they run concurrently and cannot see each other.
- **Redundant**: give the SAME hard task to 2-4 agents and use the best answer,
  or `strategy: "vote"` for short factual answers. This trades quota for speed
  and reliability. Mix Gemini tiers (e.g. one `pro` + two `lite`).

Fan-in is your job: merge results, verify for real (run the tests), and send the
next round. Parallelism does NOT make a single answer arrive faster — thinking
time is per-agent. Parallelism gives you N answers in the time of the slowest
one, and a fallback when one agent fails.

### Tools

| Tool | Purpose |
| --- | --- |
| `agy_pool_code_task` | Work order → structured JSON file edits. Main tool. |
| `agy_pool_fanout` | Fire subtasks at several agents in parallel, collect all. |
| `agy_pool_review` | Independent pass/fail verdict on finished work. |
| `agy_pool_chat` | One-off question, plain text back. |
| `agy_pool_batch` | Independent prompts in parallel across accounts. |
| `agy_pool_status` | Accounts, token expiry, cooldowns, counters. |
| `agy_pool_health` | Live probe: which models really respond. |
| `agy_pool_quota` | Remaining quota per account (cached). |
| `agy_pool_models` | Model names + aliases. |

### Models

Aliases: `default` / `flash` / `fast` / `gemini` / `pro` / `deep` all resolve to
**`gemini-3.8-flash-high`** — the strongest tier, used by default. `lite` /
`cheap` → `gemini-3.5-flash-lite` (cheapest). There are deliberately **no Claude
aliases**: Claude models burn a separate, scarcer quota bucket (`3p-5h`), so keep
every task and ensemble all-Gemini.

Measured with a 67-case execution test — every working model fixed the same
algorithm bug, so latency is the differentiator:

| Model | Hard task | Note |
| --- | --- | --- |
| `gemini-2.5-flash` | 1.7 s | fastest |
| `gemini-3-flash` | 2.5 s | best speed/quality balance |
| `gemini-3.5-flash-lite` | 2.4 s | cheapest (`lite`) |
| `claude-sonnet-4-6` | 5.3 s | works, but burns the 3p-5h bucket |
| `gemini-3.8-flash-high` | 9.5 s | default — strongest, 4x slower |

**Known dead/deprecated models** (in quota metadata but broken):
`gemini-2.5-pro` (503 no capacity), `gemini-3-flash-agent`,
`gemini-3.5-flash-low`, `gemini-3.5-flash-extra-low` — these answer **HTTP 200**
with "no longer available" text. The pool detects this and returns
`MODEL_UNAVAILABLE` instead of garbage.

### Security notes

- Account JSON files live in `~/.antigravity_tools/accounts/` and are never read
  into logs, never echoed, never copied anywhere.
- OAuth client credentials live outside any repository, in the state directory.
- When Google rotates a refresh token, the new one is written back to the same
  account file atomically (`.tmp` + `os.replace`) so nothing is lost on crash.
- Nothing in this repo contains tokens.

---

## Tiếng Việt

### Đây là gì

Một file duy nhất `agy_pool.py` (~35 KB, chỉ cần `mcp >= 2.0`) biến một kho
tài khoản Google thành endpoint Gemini có xoay vòng:

- **Gọi thẳng HTTPS** tới `daily-cloudcode-pa.googleapis.com` — đúng backend mà
  app Antigravity dùng.
- **Không tạo conversation nào** trong Antigravity/Gemini. Đã đo: 6 request →
  số conversation không đổi. Lịch sử chat sạch.
- **Tự refresh OAuth.** Token hết hạn được đổi bằng refresh token; token mới
  được ghi lại vào file account (atomic) nên agy-switch vẫn hoạt động bình
  thường.
- **Xoay vòng round-robin có cooldown.** 429 → nghỉ 5 phút, 401/403 → 15 phút,
  timeout/5xx → 60 giây. Hết account này tự nhảy sang account kia.
- **Fan-out song song.** `agy_pool_fanout` bắn các subtask độc lập ra nhiều
  account cùng lúc, thu hết kết quả trong một lần gọi (đo thật: 6 request
  trong 4.7 giây so với ~24 giây tuần tự — nhanh gấp 5 lần với việc rời nhau).

### Có cần cài agy-switch không?

**Không bắt buộc — nhưng đây là nguồn account được khuyến nghị.**

Điều kiện duy nhất: có thư mục `~/.antigravity_tools/accounts/*.json` chứa file
account hợp lệ. Nguồn có thể là:

1. **[agy-switch](https://github.com/anglee0323/agy-switch)** — khuyến nghị.
   Công cụ desktop quản lý nhiều tài khoản Google cho Antigravity: thêm account,
   refresh token, đổi account đang dùng. Thư mục dữ liệu của nó là
   `~/.antigravity_tools` — pool đọc chung đúng chỗ đó. Cài vào, thêm tài khoản
   Google, xong.
2. **Tự viết file JSON.** Bất kỳ công cụ nào tạo được file dạng:

   ```json
   {
     "id": "any-unique-id",
     "email": "you@gmail.com",
     "disabled": false,
     "token": {
       "access_token": "...",
       "refresh_token": "...",
       "expiry_timestamp": 1793176419,
       "project_id": "aicode-consumers"
     }
   }
   ```

   Chỉ cần `access_token` + `expiry_timestamp` là chạy được ngay; có thêm
   `refresh_token` + credentials OAuth (xem trên) thì pool tự refresh vô hạn.
   Nếu để chỗ khác thì set `ABV_DATA_DIR`.

### Credentials OAuth (cần cho auto-refresh)

Pool cần client id/secret của Google OAuth để đổi refresh token. Thứ tự tìm:

1. Biến môi trường `AGY_CLIENT_ID` / `AGY_CLIENT_SECRET`
2. File `%LOCALAPPDATA%/agy-pool/oauth.json` (hoặc `~/.config/agy-pool/oauth.json`)
   — nội dung `{"client_id": "...", "client_secret": "..."}`, tự tạo, không commit
3. Dự phòng: file `.env` có `AGY_CLIENT_ID=` / `AGY_CLIENT_SECRET=`

Lấy ở đâu: dùng credentials của client Antigravity, hoặc tự đăng ký OAuth client
trên Google Cloud. Không có credentials thì pool vẫn chạy — chỉ là không tự
refresh được token hết hạn.

### Cài vào OpenCode

Xem cấu hình JSON ở phần English. Mọi MCP client khác cũng dùng được (stdio).

### Vòng lặp orchestration

Pool là **bộ não**, không phải **bàn tay** — nó không được chạm vào filesystem.
Agent coding (OpenCode, Claude Code...) chạy vòng lặp:

1. **Giao task** — goal, tiêu chí nghiệm thu, nội dung file, ràng buộc.
2. **Agent tự ghi file** theo JSON trả về.
3. **Kiểm tra thật** — chạy test/build. Đừng tin cờ `done` của model.
4. **Giao task tiếp** — đẩy lỗi về cho nó sửa. Lặp đến khi xanh.

### Fan-out: nhiều agent cùng lúc (agent làm sếp)

Khi bài toán chia được, hoặc thời gian "nghĩ" là nút cổ chai, dùng
`agy_pool_fanout`: chia việc thành các subtask độc lập, bắn ra nhiều agent
Gemini song song, thu hết kết quả trong một lần gọi.

Hai cách dùng hợp lệ:

- **Chia nhỏ**: tách thay đổi lớn theo ranh giới file/module — mỗi agent lo một
  phần rời nhau, bạn merge sau. Các subtask PHẢI độc lập; chúng chạy đồng thời
  và không thấy nhau.
- **Trùng lặp**: giao CÙNG một bài khó cho 2-4 agent rồi lấy đáp án tốt nhất,
  hoặc `strategy: "vote"` cho câu hỏi ngắn. Đổi quota lấy tốc độ và độ chắc
  chắn. Trộn các bậc Gemini với nhau (ví dụ 1 `pro` + 2 `lite`).

Fan-in (ghép kết quả) là việc của bạn: merge, verify thật (chạy test), rồi mới
giao vòng tiếp. Song song KHÔNG làm một câu trả lời đến nhanh hơn — thời gian
nghĩ là của từng agent. Song song cho bạn N đáp án trong thời gian của con chậm
nhất, kèm phương án dự phòng khi một agent chết.

### Models

Alias: `default` / `flash` / `fast` / `gemini` / `pro` / `deep` đều trỏ về
**`gemini-3.8-flash-high`** — bậc mạnh nhất, dùng mặc định. `lite` / `cheap` →
`gemini-3.5-flash-lite` (rẻ nhất). Cố ý **không có alias Claude**: các model
Claude đốt bucket quota riêng (`3p-5h`) khan hiếm hơn, nên giữ mọi task và
ensemble thuần Gemini.

Model đã chết (trả HTTP 200 kèm chữ "no longer available"):
`gemini-3-flash-agent`, `gemini-3.5-flash-low`, `gemini-3.5-flash-extra-low`,
và `gemini-2.5-pro` (503). Pool tự phát hiện và trả `MODEL_UNAVAILABLE`.

### Bảo mật

- File account trong `~/.antigravity_tools/accounts/` — không bao giờ log, echo
  hay copy đi đâu.
- Credentials OAuth nằm ngoài mọi repo.
- Token xoay được ghi lại atomic vào đúng file account cũ.
- Repo này không chứa token nào.

---

## License

MIT
