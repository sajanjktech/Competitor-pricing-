# itemMapping/freeEmbedder.py

from sentence_transformers import SentenceTransformer

print("[INFO] Loading FREE local embedding model (all-MiniLM-L6-v2)...")

# Load model ONCE globally (fast)
model = SentenceTransformer("all-MiniLM-L6-v2")

print("[INFO] Free embedding model loaded successfully!")


def get_embedding(text: str):
    """Generate a local embedding (Python list)."""
    if not text:
        return []
    vector = model.encode(text)
    return vector.tolist()