"""Knowledge base for RAG: chunk markdown docs, store in Chroma, search.

Build or rebuild the index (run after editing anything in knowledge/):
    uv run python -m pipeline_copilot.knowledge_base
"""
import re
import json
from pathlib import Path
from datetime import datetime, timezone

import chromadb
from chromadb.utils.embedding_functions import DefaultEmbeddingFunction

from pipeline_copilot.config import KnowledgeSettings, load_knowledge_settings

COLLECTION = "knowledge"


# --- Chunking: one chunk per "## " section ---
# Each chunk's text starts with "<doc title> > <section heading>" (a contextual
# header), so a section like "How to check" still says WHAT it's about. That
# header is part of the embedded text, so it improves retrieval too.
def chunk_markdown(path: Path) -> list[dict]:
    text = path.read_text(encoding="utf-8")
    title_match = re.search(r"^# (.+)$", text, flags=re.MULTILINE)
    title = title_match.group(1).strip() if title_match else path.stem

    chunks = []
    # split at lines starting with "## "; part 0 is everything before the first section
    for part in re.split(r"^## ", text, flags=re.MULTILINE)[1:]:
        heading, _, body = part.partition("\n")
        heading, body = heading.strip(), body.strip()
        slug = re.sub(r"[^a-z0-9]+", "-", heading.lower()).strip("-")
        chunks.append({
            "id": f"{path.stem}#{slug}",
            "text": f"{title} > {heading}\n\n{body}",
            "metadata": {
                "source": path.name,
                "title": title,
                "section": heading,
                "type": "runbook" if path.stem.startswith("runbook_") else "reference",
            },
        })
    return chunks


def _client(settings: KnowledgeSettings) -> chromadb.ClientAPI:
    # PersistentClient stores everything in a folder on disk (.chroma/).
    # Telemetry off: the knowledge base stays fully local, no surprise network calls.
    return chromadb.PersistentClient(
        path=str(settings.chroma_path),
        settings=chromadb.config.Settings(anonymized_telemetry=False),
    )

# --- Ingestion: rebuild the collection from scratch ---
# Deleting first means removed or renamed sections can't linger as stale chunks.
def build_index(settings: KnowledgeSettings) -> list[dict]:
    chunks = [c for path in sorted(settings.knowledge_dir.glob("*.md")) for c in chunk_markdown(path)]
    client = _client(settings)
    try:
        client.delete_collection(COLLECTION)
    except Exception:
        pass   # first run: nothing to delete
    collection = client.create_collection(COLLECTION, embedding_function=DefaultEmbeddingFunction())
    # Chroma embeds each document with the embedding function as it stores it
    collection.add(
        ids=[c["id"] for c in chunks],
        documents=[c["text"] for c in chunks],
        metadatas=[c["metadata"] for c in chunks],
    )
    return chunks


# --- Retrieval: the k nearest chunks to a query ---
# distance: lower = closer in meaning. Only the ranking matters, not the value.
def search(query: str, k: int, settings: KnowledgeSettings) -> list[dict]:
    collection = _client(settings).get_collection(COLLECTION, embedding_function=DefaultEmbeddingFunction())
    res = collection.query(query_texts=[query], n_results=k)
    return [
        {"id": cid, "text": doc, "distance": dist, **meta}
        for cid, doc, dist, meta in zip(
            res["ids"][0], res["documents"][0], res["distances"][0], res["metadatas"][0]
        )
    ]


if __name__ == "__main__":
    s = load_knowledge_settings()
    chunks = build_index(s)
    print(f"Indexed {len(chunks)} chunks from {s.knowledge_dir} into {s.chroma_path}")
    for c in chunks:
        print("  ", c["id"])
        
# --- Incident memory ---
# A separate collection from the runbooks: build_index rebuilds "knowledge" only,
# so re-ingesting docs never wipes incident history.
INCIDENTS = "incidents"


def _incidents(settings: KnowledgeSettings):
    return _client(settings).get_or_create_collection(INCIDENTS, embedding_function=DefaultEmbeddingFunction())


def record_incident(diagnosis: dict, question: str, thread_id: str, settings: KnowledgeSettings) -> str:
    # One record per (UTC day, category). The FIRST diagnosis of the day is kept,
    # because it's the discovery, with the full evidence. Later re-diagnoses only
    # bump times_seen, so follow-ups ("still broken?") can't overwrite the original.
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    incident_id = f"{day}:{diagnosis['category']}"
    collection = _incidents(settings)

    existing = collection.get(ids=[incident_id])
    if existing["ids"]:
        meta = existing["metadatas"][0]
        collection.update(
            ids=[incident_id],
            metadatas=[{**meta, "times_seen": meta.get("times_seen", 1) + 1, "last_thread_id": thread_id}],
        )
        return f"{incident_id} (already recorded; seen {meta.get('times_seen', 1) + 1} times)"

    collection.add(
        ids=[incident_id],
        # What gets embedded: what a future "has this happened before?" search should match
        documents=[f"{diagnosis['category']}: {diagnosis['summary']}\nRoot cause: {diagnosis['root_cause']}"],
        # Chroma metadata values must be scalars (str/int/float/bool), so lists are joined
        metadatas=[{
            "date": day,
            "category": diagnosis["category"],
            "confidence": diagnosis["confidence"],
            "thread_id": thread_id,
            "question": question,
            "fix": diagnosis["suggested_fix"],
            "runbooks": ", ".join(diagnosis["runbooks_used"]),
            "times_seen": 1,
        }],
    )
    return incident_id



def search_incidents(query: str, k: int, settings: KnowledgeSettings) -> list[dict]:
    collection = _incidents(settings)
    count = collection.count()
    if count == 0:
        return []
    # n_results can't exceed the number of stored items
    res = collection.query(query_texts=[query], n_results=min(k, count))
    return [
        {"id": iid, "text": doc, "distance": dist, **meta}
        for iid, doc, dist, meta in zip(
            res["ids"][0], res["documents"][0], res["distances"][0], res["metadatas"][0]
        )
    ]

