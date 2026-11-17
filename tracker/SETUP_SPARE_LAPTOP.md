# Run Crypto Tracker on the spare laptop and open it from any device

The spare laptop runs the tracker all the time. It collects fresh data every 2 minutes and serves the report as a web page. From your phone or main laptop you just open a link. An open page reloads itself when new data comes in.

Prices, news, sentiment and network stats update every 2 minutes. The 1-year technicals refresh hourly and ETF/stablecoin flows every 30 minutes. That data barely changes in between, and it keeps you inside the free APIs' rate limits.

**Just want it on this computer?** Run `python crypto_tracker.py --serve --watch 2`. A browser tab opens at `http://localhost:8000`. Leave it open and it updates by itself.

## 1. Copy the folder to the spare laptop

Unzip `crypto_tracker_server.zip` anywhere on the spare laptop, for example in `Documents\crypto_tracker`.
The folder holds `crypto_tracker.py`, `requirements.txt`, `start_tracker.bat` (Windows) and `start_tracker.sh` (Mac/Linux).

## 2. Install Python (once)

- **Windows:** install Python 3 from python.org. On the first installer screen, tick **"Add python.exe to PATH"**.
- **Mac:** run `python3 --version` in Terminal. If Python is missing, macOS offers to install it.
- **Linux:** usually already installed. On Ubuntu or Debian you may also need `sudo apt install python3-venv`.

## 3. Start it

- **Windows:** double-click `start_tracker.bat`. The first time, Windows Firewall asks about Python. Choose **Allow on private networks**.
- **Mac/Linux:** in Terminal, run `cd` into the folder, then `chmod +x start_tracker.sh` (first time only), then `./start_tracker.sh`.

The window prints the addresses to open, for example:

```
this laptop:     http://localhost:8000
other devices:   http://192.168.0.23:8000
```

Leave that window open. The first snapshot takes about a minute.

## 4. Open it from your other device

**Same Wi-Fi as the spare laptop:** open the `http://192.168.x.x:8000` address on your phone or other laptop and bookmark it.

**From anywhere (university, mobile data):** use Tailscale. It's free for personal use and keeps the page private to your own devices.
1. Install Tailscale on the spare laptop and on your phone or other laptop (tailscale.com/download). Sign in to the same account on all of them.
2. On your phone, open `http://<spare-laptop-name>:8000`. You can also use the `100.x.x.x` address that the Tailscale app shows for the spare laptop.

Do **not** set up port forwarding on your router to put this on the open internet. Tailscale does the same job safely.

## 5. Keep it running 24/7

**Keep the laptop awake** (plug it in):
- **Windows:** go to Settings → System → Power and set *Sleep when plugged in* to **Never**. The screen can still turn off. If you'll close the lid, go to Control Panel → Power Options → *Choose what closing the lid does* and set it to **Do nothing** when plugged in.
- **Mac:** the start script already keeps the Mac awake while the tracker runs. With the lid closed, a Mac only stays awake if a monitor is attached, so leave the lid open.
- **Linux:** turn off automatic suspend in Settings → Power, and set the lid action to ignore in `/etc/systemd/logind.conf` (`HandleLidSwitch=ignore`).

**Start automatically after a reboot:**
- **Windows:** press Win+R, type `shell:startup`, and press Enter. Put a shortcut to `start_tracker.bat` in that folder. It starts when you log in.
- **Mac:** go to System Settings → General → Login Items and add `start_tracker.sh`. You can also open it once from Terminal after each reboot.
- **Linux:** run `crontab -e` and add `@reboot cd /path/to/folder && ./start_tracker.sh >> tracker.log 2>&1`.

## Changing settings

Edit the last line of the start script that runs `crypto_tracker.py`:

| Want | Add / change |
|---|---|
| Prices in won | `--currency krw` |
| Different refresh interval | change `--watch 2` (minutes; 2 is the lowest sensible value for the free APIs) |
| Different port | `--serve 8080` |
| Faster, lighter runs | `--no-history` and/or `--no-news` |

Other useful addresses: `/snapshot.json` serves the raw data. All the CSV files keep filling up in the `data` folder on the spare laptop, as before. Only the newest 672 timestamped reports are kept in `reports`, which is about a day at 2 minutes.

## If something doesn't work

- **The phone can't open the address but the laptop can:** check that both are on the same Wi-Fi. On Windows, also check that the network is set to **Private**: open Settings → Network & internet → Wi-Fi → your network. The firewall must allow Python too. Some guest or university Wi-Fi networks block devices from reaching each other. Use Tailscale there.
- **"Address already in use":** the tracker is already running in another window, or another program is using port 8000. Close the other window or use `--serve 8080`.
- **Prices show "–":** CoinGecko's rate limit was probably hit. The next cycle usually fixes it.
