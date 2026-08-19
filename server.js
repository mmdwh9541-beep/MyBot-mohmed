const express = require('express');
const axios = require('axios');

const app = express();
app.use(express.json()); // To read data from TradingView

// --- 1. Telegram Credentials ---
const TELEGRAM_TOKEN = '8956340113:AAGyr_IZdKMniNLYPeTnl-NoUzZzbI5hAiI';
const CHAT_ID = '8708481752';

// --- 2. Function to Send Telegram Message ---
async function sendTelegramMessage(text) {
    const url = `https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`;
    try {
        await axios.post(url, {
            chat_id: CHAT_ID,
            text: text,
            parse_mode: 'HTML'
        });
        console.log('✅ Success: Message sent to Telegram');
    } catch (error) {
        console.error('❌ Error: Could not send to Telegram', error.message);
    }
}

// --- 3. Root Route (For UptimeRobot to keep server awake) ---
app.get('/', (req, res) => {
    res.send('LOMY Bot Server is UP and Running 24/7!');
});

// --- 4. Webhook Route (Receives Alerts from TradingView) ---
app.post('/webhook', (req, res) => {
    const alertData = req.body;

    console.log('Received New Alert:', alertData);

    // Format the message that will be sent to Telegram
    const messageText = `
    🚨 <b>LOMY Trade Alert</b> 🚨
    <b>Symbol:</b> ${alertData.symbol || 'N/A'}
    <b>Action:</b> ${alertData.action || 'N/A'}
    <b>Price:</b> ${alertData.price || 'N/A'}
    `;

    // Send the message
    sendTelegramMessage(messageText);

    res.status(200).send('Alert Received and Sent to Telegram');
});

// --- 5. Start the Server ---
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`🚀 Server is running on port ${PORT}`);
});
