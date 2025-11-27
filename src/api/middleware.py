def require_api_key(request):
    """Middleware simples para validar chave de API interna."""
    api_key = request.headers.get("x-api-key")

    if not api_key:
        return {"error": "API Key ausente"}, 401

    # TODO: futuramente validar chave no Firestore
    if api_key != "dev-local-key":
        return {"error": "API Key inválida"}, 403

    return None