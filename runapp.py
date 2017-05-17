from onc_app import app_onc
import os

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app_onc.run(host='0.0.0.0', port=port, debug=True)