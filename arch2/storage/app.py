from flask import Flask, request, jsonify, send_file
import os
import requests

app = Flask(__name__)

STORAGE_PATH = "/storage"
# METADATA_API = "http://metadata:5005/files"
METADATA_API = os.environ.get("METADATA_API", "http://metadata-gateway:5005") + "/files"

os.makedirs(STORAGE_PATH, exist_ok=True)

# ---------------- Upload ----------------
@app.route("/upload", methods=["POST"])
def upload_file():
    if "file" not in request.files:
        return jsonify({"error": "No file part"}), 400
    f = request.files["file"]

    # Get username/password
    # username = request.form.get("user") or request.values.get("user")
    # password = request.form.get("password") or request.values.get("password")
    # if not username or not password:
    #     return jsonify({"error": "Username and password are required"}), 400

    print("request.form:", request.form)
    print("request.files:", request.files)
    print("request.values:", request.values)

    # Save file
    save_path = os.path.join(STORAGE_PATH, f.filename)
    try:
        f.save(save_path)
    except Exception as e:
        return jsonify({"error": f"Failed to save file: {e}"}), 500

    # Build metadata
    size = os.path.getsize(save_path)
    print("Saving file to metadata...")
    metadata = {
        "filename": f.filename,
        "path": save_path,
        "size": size,
        "version": 1,
        # "user": username,
        # "password": password
    }

    # Send metadata to metadata container
    try:
        r = requests.post(METADATA_API, json=metadata)
        r.raise_for_status()
    except Exception as e:
        return jsonify({"error": f"Failed to save metadata: {e}"}), 500

    return jsonify({"path": save_path, "status": "saved"}), 200

# ---------------- Download ----------------
@app.route("/download", methods=["GET"])
def download_file():
    filename = request.args.get("filename")
    # username = request.args.get("user")
    # password = request.args.get("password")

    # if not filename or not username or not password:
    #     return jsonify({"error": "Filename, username, and password required"}), 400

    # Fetch metadata
    try:
        r = requests.get(f"{METADATA_API}/{filename}")
        r.raise_for_status()
        metadata = r.json()
    except Exception as e:
        return jsonify({"error": f"Failed to fetch metadata: {e}"}), 404

    # Validate username/password
    # if username.strip() != metadata["user"].strip() or password.strip() != metadata["password"].strip():
    #     return jsonify({"error": "Invalid username or password"}), 403

    # Check if file exists
    file_path = metadata["path"]
    if not os.path.exists(file_path):
        return jsonify({"error": "File not found"}), 404

    return send_file(file_path, as_attachment=True)

# ---------------- Delete ----------------
@app.route("/delete", methods=["DELETE"])
def delete_file():
    filename = request.args.get("filename")
    # username = request.args.get("user")
    # password = request.args.get("password")

    # if not filename or not username or not password:
    #     return jsonify({"error": "Filename, username, and password required"}), 400

    # Fetch metadata
    try:
        print("Going to metadata gateway for deletion")
        r = requests.get(f"{METADATA_API}/{filename}")
        r.raise_for_status()
        metadata = r.json()
    except Exception as e:
        return jsonify({"error": f"Failed to fetch metadata: {e}"}), 404

    # # Validate username/password
    # if username.strip() != metadata["user"].strip() or password.strip() != metadata["password"].strip():
    #     return jsonify({"error": "Invalid username or password"}), 403

    # Delete file
    try:
        file_path = metadata["path"]
        if os.path.exists(file_path):
            os.remove(file_path)
            print("Permanent storage deletion applied")
    except Exception as e:
        print("Issue with deep delete")
        return jsonify({"error": f"Failed to delete file: {e}"}), 500

    # Delete metadata
    try:
        r = requests.delete(f"{METADATA_API}/{filename}")
        r.raise_for_status()
    except Exception as e:
        return jsonify({"error": f"Failed to delete metadata: {e}"}), 500
    print("Deleted from metadata...")
    return jsonify({"status": "deleted"}), 200

# ---------------- Main ----------------
if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(line_buffering=True)  # ensure prints appear immediately
    app.run(host="0.0.0.0", port=5006, debug=True)
