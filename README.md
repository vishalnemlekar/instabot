# Instabot — Instamart Scraper + Telegram Discount Bot

This repository contains two main tools:

1. **Instamart Scraper (`instamart_scraper_tiles.py`)**  
   Scrapes Instamart product data (parent + tiles) and stores it into Supabase.

2. **Telegram Discount Bot (`bot.py`)**  
   Watches the Supabase database and alerts a Telegram channel/group whenever a product’s discount crosses **70% OFF**.

---

## ✨ Features

- Scraper
  - Captures products from Instamart parent categories and tiles.
  - Saves fields like `brand`, `discount`, `mrp`, `offer_price`, `productId`, `sku`, `store_price`, `var_id`, `tile_id`, `tile_name`, `category`.
  - Updates Supabase (`instamart_products` table).

- Bot
  - Polls Supabase every `POLL_MINUTES` (default 10).
  - Sends Telegram messages with formatted details if a discount ≥70% is found.
  - Avoids duplicate spam with a local JSON cache.

---

## 🛠 Requirements

- Python **3.12+**
- Supabase project + table `instamart_products`
- Telegram Bot token ([get from @BotFather](https://t.me/BotFather))
- Telegram chat/channel ID

---

## ⚙️ Setup

### Clone repo
```bash
git clone https://github.com/vishalnemlekar/instabot.git
cd instabot
