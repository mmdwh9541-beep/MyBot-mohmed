const express = require('express');
const cors = require('cors');
const TelegramBot = require('node-telegram-bot-api');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

// Telegram Bot Setup using Render Environment Variables
const token = process.env.TELEGRAM_BOT_TOKEN;
const chatId = process.env.TELEGRAM_CHAT_ID;

let bot = null;
if (token) {
    bot = new TelegramBot(token, { polling: false });
    console.log("Telegram Bot initialized successfully.");
} else {
    console.log("Telegram Bot Token is missing!");
}

// Function to send alerts in English
function sendTelegramAlert(message) {
    if (bot && chatId) {
        bot.sendMessage(chatId, message).catch(err => console.log("Telegram Error:", err.message));
    }
}

app.get('/', (req, res) => {
    res.send(`
        <html>
            <head>
                <title>Trading Bot Dashboard</title>
                <style>
                    body { background: #121212; color: white; text-align: center; padding: 50px; font-family: Arial; }
                    h1 { color: #089981; }
                    p { font-size: 18px; color: #aaaaaa; }
                </style>
            </head>
            <body>
                <h1>System is Online!</h1>
                <p>Dashboard is ready and Telegram alerts are integrated.</p>
            </body>
        </html>
    `);
});

app.listen(PORT, () => {
    console.log(`System Running on port ${PORT}`);
    // Send a startup notification test to Telegram
    sendTelegramAlert("🚀 Trading Bot System is Online and Connected Successfully!");
});
