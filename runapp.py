from bdnyc_app import app_bdnyc
import os

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app_bdnyc.run(host='0.0.0.0', port=port, debug=False)