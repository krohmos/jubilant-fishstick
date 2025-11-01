import html
import os
import subprocess

from flask import Flask, redirect, render_template_string, request

app = Flask(__name__)

FORM = """
<!doctype html>
<title>Wi-Fi Setup</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<h2>Connect CaptainPi to Wi-Fi</h2>
<form method="post">
  <label>SSID:<br><input name="ssid" required></label><br><br>
  <label>Password:<br><input name="psk" type="password" required></label><br><br>
  <button type="submit">Save & Reboot</button>
</form>
<p>Tip: After reboot, reconnect your phone to your normal Wi-Fi.</p>
"""

OK = """
<!doctype html>
<title>Saved</title>
<h2>Saved. Rebooting now…</h2>
"""


@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        ssid = request.form.get("ssid", "").strip()
        psk = request.form.get("psk", "").strip()

        if not ssid or not psk:
            return "Missing SSID or password", 400

        wpa = f'''country=NO
ctrl_interface=DIR=/var/run/wpa_supplicant GROUP=netdev
update_config=1

network={{
    ssid="{ssid.replace('"','\\"')}"
    psk="{psk.replace('"','\\"')}"
}}
'''
        # Write Wi-Fi config
        with open("/etc/wpa_supplicant/wpa_supplicant.conf", "w") as f:
            f.write(wpa)

        # Ensure permissions
        os.chmod("/etc/wpa_supplicant/wpa_supplicant.conf", 0o600)

        # Leave setup mode: remove marker
        try:
            os.remove("/var/lib/harborpi/setup_mode")
        except FileNotFoundError:
            pass

        # Reboot
        subprocess.Popen(["/sbin/reboot"])
        return OK

    return render_template_string(FORM)


def main():
    # Bind to setup IP
    app.run(host="0.0.0.0", port=80)


if __name__ == "__main__":
    main()
