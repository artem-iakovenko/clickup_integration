from flask import Flask, request, jsonify
import threading
from datetime import datetime
from available_resources import available_resources_collector

app = Flask(__name__)


@app.route('/available_resources_sync', methods=['POST'])
def calculate_resource():
    trigger_date = datetime.now()
    current_date = trigger_date.strftime('%Y-%m-%d')
    msg = f"Internal resources sync has been called"
    print(msg)
    thread = threading.Thread(target=available_resources_collector, args=(current_date, ))
    thread.start()
    return jsonify({'status': msg, 'trigger_date': trigger_date})


app.run(host='0.0.0.0', port=4010)
