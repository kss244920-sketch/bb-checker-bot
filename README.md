# BigBasket Enterprise Bulk Scanner & Auto-OTP Bot

Automated 24/7 100% Pure Lightweight HTTP REST API BigBasket wallet, Free Cash & voucher checker bot with Render Keep-Alive, state recovery/persistence, rotating proxy support, and multi-Firebase panel parallel scanning.

## ✨ Features
- 💰 **Comprehensive Balance & Promotional Extraction (100% Pure HTTP REST API - 0 Browser)**:
  - **Free Cash Balance**: Direct extraction of BigBasket promotional gifts, cashback, and shopping credits.
  - **Free Cash Expiry & Min Order**: Auto-detects promo expiration dates (`valid_till`) and minimum order value restrictions.
  - **Total Balance**: Complete usable funds.
  - **Main Wallet**: Real deposited/refunded cash balance.
  - **Tata NeuCoins**: NeuCoins reward points breakdown.
  - **Address & Location**: Member's primary delivery city and pincode via BB member address API.
  - **1-Click Cookie Login**: Generates instant browser session cookie string (`_bb_mid`, `BBAUTHTOKEN`, `customer_hash`, `csurftoken`) for 1-click web login without OTP.
- ⚡ **Multi-Panel Scanner Hub**: Real-time multi-threaded scanning across Firebase RTDB panels with auto OTP capture and verification.
- 🛡️ **24-Hour Deduplication Cache**: Remembers processed numbers to prevent burning panel OTPs repeatedly.
- 🌐 **Rotating HTTP Proxy Support**: Direct curl_cffi HTTP proxy rotation via env var (`PROXIES`, `PROXY_URL`) or `config/proxies.txt` to eliminate 429 rate-limiting.
- 📱 **Single Number Check**: Instant 1-on-1 verification with detailed Free Cash and wallet card.
- 📥 **Dual Export Hub**: Generates both human-readable `.txt` report and Excel-ready `.csv` spreadsheet with separate columns.
- 🔄 **24/7 Render Keep-Alive & Auto-Recovery**: Disaster recovery with silent cloud backup and auto-restart resilience.
