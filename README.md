Instamart Scraper + Discount Telegram Bot

This repo has two components:

Scraper (instamart_scraper_tiles.py)

Scrapes product data (including tiles/categories) from Instamart.

Stores results in Supabase (instamart_products table).

Telegram Bot (bot.py)

Watches the Supabase table for products ≥70% discount.

Sends formatted alerts to a Telegram chat/channel.

Features

Scraper:

Captures parent + tile products

Extracts fields: brand, discount, mrp, name, offer_price, productId, sku, store_price, var_id, tile_id, tile_name, category

Saves into Supabase

Bot:

Runs as a background worker

Polls every POLL_MINUTES minutes

Notifies Telegram if a product crosses 70% discount (or discount increases)

Avoids duplicate spam by caching sent alerts

Requirements

Python 3.12+

Supabase project + table instamart_products

Telegram bot token from @BotFather

Telegram chat ID (channel/group/user)

Setup
1. Clone repo
git clone https://github.com/your-username/instamart-discount-bot.git
cd instamart-discount-bot

2. Install dependencies
pip install -r requirements.txt

3. Configure environment

Create a .env file:

TELEGRAM_BOT_TOKEN=your-telegram-bot-token
TELEGRAM_CHAT_ID=123456789
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_ROLE_KEY=your-supabase-service-role-key
SUPABASE_TABLE=instamart_products
POLL_MINUTES=10

Running
Scraper
python instamart_scraper_tiles.py


This will populate/update the instamart_products table in Supabase.

Bot
python bot.py


This will run the Telegram discount watcher.
