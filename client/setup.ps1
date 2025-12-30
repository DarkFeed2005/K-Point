# Client Setup Script for Windows
# Run this from the K-Point/client directory

Write-Host "Setting up Reverse Tunnel Client..." -ForegroundColor Green

# Install Tailwind CSS
Write-Host "`n1. Installing Tailwind CSS..." -ForegroundColor Yellow
npm install -D tailwindcss postcss autoprefixer
npx tailwindcss init -p

# Create Tailwind Config
Write-Host "`n2. Creating Tailwind configuration..." -ForegroundColor Yellow
@"
/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {},
  },
  plugins: [],
}
"@ | Out-File -FilePath "tailwind.config.js" -Encoding UTF8

# Update CSS file
Write-Host "`n3. Updating styles..." -ForegroundColor Yellow
@"
@tailwind base;
@tailwind components;
@tailwind utilities;
"@ | Out-File -FilePath "src\styles.css" -Encoding UTF8

# Update main.tsx
Write-Host "`n4. Creating main.tsx..." -ForegroundColor Yellow
@"
import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
import "./styles.css";

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
"@ | Out-File -FilePath "src\main.tsx" -Encoding UTF8

# Install all dependencies
Write-Host "`n5. Installing npm dependencies..." -ForegroundColor Yellow
npm install

Write-Host "`n✅ Setup complete!" -ForegroundColor Green
Write-Host "`nNext steps:" -ForegroundColor Cyan
Write-Host "1. Copy the Tauri backend code to src-tauri\src\main.rs"
Write-Host "2. Copy the React frontend code to src\App.tsx"
Write-Host "3. Run: npm run tauri dev"