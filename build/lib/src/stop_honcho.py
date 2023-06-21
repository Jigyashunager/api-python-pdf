import subprocess

# Stop Honcho processes gracefully
subprocess.run(['taskkill', '/F', '/IM', 'honcho.exe'])
