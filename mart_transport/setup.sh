#!/bin/bash
# Setup script for mart transport

echo "🔧 Setting up mart transport..."

# Install dependencies
echo "📦 Installing dependencies..."
pip install -r requirements.txt

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "📝 Creating .env file..."
    cat > .env << EOF
# MotherDuck Configuration
MD_TOKEN=your_motherduck_token_here

# MySQL Configuration
MYSQL_HOST=localhost
MYSQL_USER=root
MYSQL_PASSWORD=your_mysql_password
MYSQL_DB=rri_data
EOF
    echo "✅ Created .env file - please update with your credentials"
else
    echo "✅ .env file already exists"
fi

echo "✅ Setup complete!"
echo ""
echo "To run the transport:"
echo "1. Update .env with your credentials"
echo "2. Run: python transporter.py"
