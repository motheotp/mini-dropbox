import os
import jwt
import datetime
from werkzeug.security import generate_password_hash, check_password_hash
from flask import Flask, request, jsonify, Response
import requests, os

app = Flask(__name__)

# METADATA_API = "http://metadata:5005" # metadata service URL

METADATA_API = os.environ.get("METADATA_API", "http://metadata-gateway:5005")
STORAGE_API = "http://storage:5006" # storage service URL
SECRET_KEY = os.environ.get("SECRET_KEY", "supersecretkey") # secret key for JWT - in more secure setup, use env variable


# --- JWT Helpers ---
def decode_token(token):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        return payload["sub"]
    except Exception:
        return None

# auth decorator
def require_auth(f):
    def wrapper(*args, **kwargs):
        # grab the header
        auth_header = request.headers.get("Authorization")

        # check if auth header is present and valid
        if not auth_header or not auth_header.startswith("Bearer "):
            return jsonify({"error": "Missing or invalid token"}), 401
        
        # decode the token
        token = auth_header.split(" ", 1)[1]
        username = decode_token(token)
        if not username:
            return jsonify({"error": "Invalid or expired token"}), 401
        request.username = username
        return f(*args, **kwargs)
    wrapper.__name__ = f.__name__
    return wrapper


# download file endpoint
@app.route("/files/download", methods=["GET"])
@require_auth
def download():
    # get the filename from query parameters
    filename = request.args.get("filename")
    if not filename:
        return jsonify({"error": "No filename provided"}), 400

    # forward request to storage service via GET
    params = {"filename": filename}
    resp = requests.get(f"{STORAGE_API}/download", params=params, stream=True)

    # check response from storage service
    if resp.status_code == 200:
        return Response(
            resp.iter_content(chunk_size=8192),
            content_type=resp.headers.get('Content-Type'),
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    else:
        try:
            return jsonify(resp.json()), resp.status_code
        except Exception:
            return jsonify({"error": "File not found - " + resp.text}), 404

# delete file endpoint
@app.route("/files/delete", methods=["DELETE"])
@require_auth
def delete_file():
    # get the filename from query parameters
    filename = request.args.get("filename")
    if not filename:
        return jsonify({"error": "No filename provided"}), 400
    
    # forward request to storage service via DELETE
    params = {"filename": filename}
    resp = requests.delete(f"{STORAGE_API}/delete", params=params)
    # check response from metadata service
    if resp.status_code == 200:
        return resp.json(), resp.status_code
    else:
        return jsonify({"error": "Delete error - " + resp.text}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5004)