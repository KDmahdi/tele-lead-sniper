Markdown
# Telegram Outreach Listener & Project Scout 🚀

An asynchronous Python tool built with [Telethon](https://github.com/LonamiWebs/Telethon) designed to monitor target Telegram channels, detect employer/client posts, extract contact handles, and perform automated, rate-limited initial outreach.

---

## ✨ Features

- **Real-Time Monitoring**: Listens to specified public/private channels or groups concurrently.
- **Negative Filtering**: Normalizes Persian/Arabic text to eliminate unwanted posts (e.g., job seekers, typists, spam).
- **Handle Extraction & Validation**: Extracts `@username` and `t.me/` handles, validating that targets are real user accounts (ignoring bots, channels, and groups).
- **Stateful SQLite Deduplication**: Tracks outreach state locally (`history.db`) to ensure prospective clients are never messaged more than once.
- **Human-like Delay & Anti-Flood**: Adds randomized pauses between interactions and handles Telegram RPC / `FloodWait` exceptions gracefully.
- **Saved Messages Logging**: Delivers detailed summaries directly to your Telegram *Saved Messages* for instant tracking.
- **Dry-Run Mode**: Safely test filters and extraction without sending any direct messages.

---

## 🛠️ Tech Stack

- **Python 3.10+**
- **Telethon** (MTProto Telegram client)
- **SQLite3** (Lightweight persistence)
- **Asyncio** (High-concurrency event loop)

---

## 🚀 Quick Start

### 1. Clone the repository
```bash
git clone [https://github.com/](https://github.com/)<your-username>/<repo-name>.git
cd <repo-name>
2. Install dependencies
Bash
pip install -r requirements.txt
3. Setup Credentials & Configuration
Open tg_listener.py and provide your credentials (or load them from environment variables):

Python
API_ID = 12345678          # Your Telegram API ID
API_HASH = "your_api_hash" # Your Telegram API Hash
You can obtain these from my.telegram.org.

Update TARGET_CHANNELS, BLACKLIST_WORDS, and MESSAGE_TEMPLATE:

Note: Leaving MESSAGE_TEMPLATE = "" automatically activates DRY-RUN mode (contacts are extracted and saved to SQLite without sending messages).

4. Run
Bash
python tg_listener.py
On the first run, Telethon will ask for your phone number and login code to create a local session.

📂 Project Structure
Plaintext
├── tg_listener.py     # Main application & event pipeline
├── requirements.txt   # Telethon dependency
├── history.db         # SQLite database (auto-generated)
├── listener.log       # Rotating event logs (auto-generated)
└── README.md          # Documentation
⚠️ Disclaimer
This tool is created for educational and workflow-automation purposes. Ensure you comply with Telegram's Terms of Service and Anti-Spam policies when using automated user clients.
