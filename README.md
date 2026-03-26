# COKACDIR

**AI Coding Agent You Can Run From Telegram.**

Turn any Telegram chat into a full AI coding environment. Send a message to your bot, and it executes code, edits files, runs shell commands, and streams results back in real time — all from your phone or any device with Telegram. Supports Claude Code, Codex CLI, Gemini CLI, and OpenCode as AI backends with seamless cross-provider session management.

Also works as a standalone multi-panel terminal file manager with AI-powered natural language commands — press `.` and describe what you want.

## Features

- **Blazing Fast**: Written in Rust for maximum performance. Single binary (15-20MB depending on platform), optimized with LTO and strip.
- **AI-Powered Commands**: Natural language coding and file management powered by Claude, Codex, Gemini & OpenCode. Press `.` and describe what you want.
- **Multi-Panel Navigation**: Dynamic multi-panel interface for efficient file management
- **Keyboard Driven**: Full keyboard navigation designed for power users
- **Built-in Editor**: Edit files with syntax highlighting for 20+ languages
- **Image Viewer**: View images directly in terminal (Kitty, iTerm2, Sixel protocols) with zoom and pan
- **Process Manager**: Monitor and manage system processes with sortable columns
- **File Search**: Find files by name pattern with recursive search
- **Diff Compare**: Side-by-side folder and file comparison
- **Git Integration**: Built-in git status, commit, log, branch management and inter-commit diff
- **Remote SSH/SFTP**: Browse remote servers via SSH/SFTP with saved profiles
- **File Encryption**: AES-256 encryption with configurable chunk splitting
- **Duplicate Finder**: Detect and manage duplicate files with hash-based comparison
- **Telegram Bot**: Control your AI coding sessions remotely via Telegram with streaming output
- **Customizable Themes**: Light/Dark themes with full JSON-based color customization

## Installation & AI Setup

For installation instructions, AI provider setup, and keyboard shortcuts, visit:

**[https://cokacdir.cokac.com](https://cokacdir.cokac.com)**

Supports 4 AI providers: **Claude Code**, **Codex CLI**, **Gemini CLI**, and **OpenCode**.

## Telegram Bot

Run your AI coding sessions remotely via Telegram:

```bash
cokacdir --ccserver <TELEGRAM_BOT_TOKEN> [TOKEN2] ...
```

**Capabilities:**
- Multi-provider support (Claude, Codex, Gemini, OpenCode) with live streaming
- Session persistence and cross-provider session resolution
- Scheduled tasks with cron expressions or absolute times
- Group chat support with shared context across multiple bots
- Bot-to-bot messaging for multi-agent workflows
- File upload/download, tool management, and debug logging

**Commands:** `/start`, `/stop`, `/clear`, `/help`, `/session`, `/pwd`, `/model`, `/down`, `/instruction`, `/instruction_clear`, `/allowed`, `/allowedtools`, `/availabletools`, `/context`, `/query`, `/public`, `/direct`, `/setpollingtime`, `/debug`, `/silent`

## Supported Platforms

- macOS (Apple Silicon & Intel)
- Linux (x86_64 & ARM64)
- Windows (x86_64 & ARM64)

## License

MIT License

## Author

cokac <monogatree@gmail.com>

Homepage: https://cokacdir.cokac.com

## Disclaimer

THIS SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.

IN NO EVENT SHALL THE AUTHORS, COPYRIGHT HOLDERS, OR CONTRIBUTORS BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

This includes, without limitation:

- Data loss or corruption
- System damage or malfunction
- Security breaches or vulnerabilities
- Financial losses
- Any direct, indirect, incidental, special, exemplary, or consequential damages

The user assumes full responsibility for all consequences arising from the use of this software, regardless of whether such use was intended, authorized, or anticipated.

**USE AT YOUR OWN RISK.**
