#!/usr/bin/env python3
"""Trading System UI Dashboard Server (Simple HTTP Server)

Usage:
    python3 ui/dashboard_server.py
    python3 ui/dashboard_server.py --host 0.0.0.0 --port 8080
    
Requirements: Python 3.6+ (no dependencies)
"""

import http.server
import socketserver
import os
import argparse


def get_dashboard_path():
    """Get absolute path to dashboard HTML file."""
    paths = [
        'ui/dashboard.html',
        './ui/dashboard.html',
    ]
    
    for path in paths:
        if os.path.exists(path):
            return os.path.abspath(path)
    
    # Create placeholder
    print(f"Creating placeholder at {paths[0]}")
    with open(paths[0], 'w') as f:
        f.write('<html><body><h1>Trading System Dashboard</h1><p>Deploying...</p></body></html>')
    return os.path.abspath(paths[0])


def parse_args():
    parser = argparse.ArgumentParser(description='Simple HTTP server for Trading System Dashboard')
    parser.add_argument('--host', '-b', default='0.0.0.0', help='Bind address (default: 0.0.0.0)')
    parser.add_argument('--port', '-p', type=int, default=8000, help='Port (default: 8000)')
    return parser.parse_args()


def main():
    args = parse_args()
    
    print("Trading System UI Dashboard Server")
    print("=" * 50)
    print(f"Dashboard file: {get_dashboard_path()}")
    print(f"Starting server at http://{args.host}:{args.port}/dashboard")
    print("=" * 50)
    print("")
    print("Press Ctrl+C to stop")
    
    # Run HTTP server
    with socketserver.TCPServer(("", args.port), http.server.SimpleHTTPRequestHandler) as httpd:
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down...")


if __name__ == "__main__":
    main()
