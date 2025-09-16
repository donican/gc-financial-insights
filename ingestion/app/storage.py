from __future__ import annotations
import io, json, os
from typing import Optional
from urllib.parse import urlparse

try:
    from google.cloud import storage as gcs 
except Exception:
    gcs = None

def _is_gcs(uri: str) -> bool:
    return uri.startswith("gs://")

def _split_gs(uri: str):
    """
    Retorna (bucket, base) a partir de uma URI gs://bucket[/base]
    Exemplos:
      gs://my-bkt                  -> ("my-bkt", "")
      gs://my-bkt/                 -> ("my-bkt", "")
      gs://my-bkt/prefix/sub       -> ("my-bkt", "prefix/sub")
    """
    if not _is_gcs(uri):
        raise ValueError(f"URI inválida (esperado gs://...): {uri!r}")
    rest = uri[len("gs://"):].strip()         # remove 'gs://' e espaços
    if not rest:
        raise ValueError(f"Bucket ausente em {uri!r}")
    parts = rest.split("/", 1)
    bucket = parts[0].strip()
    base = parts[1].strip("/") if len(parts) == 2 else ""
    if not bucket:
        raise ValueError(f"Bucket ausente em {uri!r}")
    return bucket, base

class Storage:
    def __init__(self, output_uri: str):
        self.output_uri = output_uri
        if _is_gcs(output_uri) and gcs is None:
            raise RuntimeError("google-cloud-storage não instalado/configurado")

        self._gcs_client = gcs.Client() if (_is_gcs(output_uri) and gcs) else None

    def write_parquet(self, df, path: str):
        import pandas as pd 
        if _is_gcs(self.output_uri):
            bucket, base = _split_gs(self.output_uri)
            prefix = f"{base.strip('/')}/" if base else ""
            blob_path = f"{prefix}{path.lstrip('/')}"
            buf = io.BytesIO()
            df.to_parquet(buf, index=False)
            buf.seek(0)
            self._gcs_client.bucket(bucket).blob(blob_path).upload_from_file(buf, content_type="application/octet-stream")
        else:
            full = os.path.join(self.output_uri, path)
            os.makedirs(os.path.dirname(full), exist_ok=True)
            df.to_parquet(full, index=False)

    def read_json(self, path: str) -> Optional[dict]:
        try:
            if _is_gcs(self.output_uri):
                bucket, base = _split_gs(self.output_uri)
                prefix = f"{base.strip('/')}/" if base else ""
                blob_path = f"{prefix}{path.lstrip('/')}"
                blob = self._gcs_client.bucket(bucket).blob(blob_path)
                if not blob.exists():
                    return None
                return json.loads(blob.download_as_text())
            else:
                full = os.path.join(self.output_uri, path)
                if not os.path.exists(full):
                    return None
                with open(full, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception:
            return None

    def write_json(self, obj: dict, path: str):
        if _is_gcs(self.output_uri):
            bucket, base = _split_gs(self.output_uri)
            prefix = f"{base.strip('/')}/" if base else ""
            blob_path = f"{prefix}{path.lstrip('/')}"
            self._gcs_client.bucket(bucket).blob(blob_path).upload_from_string(
                json.dumps(obj, ensure_ascii=False), content_type="application/json"
            )
        else:
            full = os.path.join(self.output_uri, path)
            os.makedirs(os.path.dirname(full), exist_ok=True)
            with open(full, "w", encoding="utf-8") as f:
                json.dump(obj, f, ensure_ascii=False)
