import trustme

def create_certs():

    ca = trustme.CA()
    cert = ca.issue_cert("localhost")

    # For use by clients (eg 'verify' in HTTPX)
    ca.cert_pem.write_to_path("server_data/certs/ca.pem")

    # For use by servers (eg '--ssl-certfile' in Uvicorn)
    cert.private_key_and_cert_chain_pem.write_to_path("server_data/certs/server.pem")
