# PROJECT_INFO.md

## Project Name
المحاسبة السعودية العالمية (Saudi Accounting Global)

## Project Description and Specifications
This project is focused on developing a comprehensive accounting solution tailored for the needs of businesses operating in Saudi Arabia. It includes features for bookkeeping, tax calculations, and financial reporting, ensuring compliance with local regulations.

## Technology Stack
- **Frontend:** React.js, Bootstrap
- **Backend:** Node.js, Express
- **Database:** MongoDB
- **Deployment:** Docker, AWS

## Configuration Information
- **Environment Variables:**
  - `DB_CONNECTION_STRING`: MongoDB connection string.
  - `API_KEY`: Your API key for third-party integrations.

## Environment Setup
1. Clone the repository:
   ```bash
   git clone https://github.com/sad770620043-art/saudi-accounting.git
   ```
2. Navigate to the project directory:
   ```bash
   cd saudi-accounting
   ```
3. Install dependencies:
   ```bash
   npm install
   ```
4. Set up environment variables based on the `.env.example` file.
5. Start the application:
   ```bash
   npm start
   ```

## Project Structure
```
saudi-accounting/
│
├── client/                # Frontend code
│   ├── src/
│   └── public/
│
├── server/                # Backend code
│   ├── models/
│   └── routes/
│
├── .env.example           # Environment variable template
├── README.md              # Project README
└── PROJECT_INFO.md        # Project documentation
```