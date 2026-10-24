"""匿名检查 GHCR 镜像是否公开可拉:python tools/check_public.py [镜像全名]"""
import json
import sys
import urllib.error
import urllib.request

image = sys.argv[1] if len(sys.argv) > 1 else "yizhixiaoliulang/sse-forwarder"
tok = json.load(urllib.request.urlopen(
    f"https://ghcr.io/token?scope=repository:{image}:pull"))["token"]
req = urllib.request.Request(
    f"https://ghcr.io/v2/{image}/manifests/latest",
    headers={"Authorization": "Bearer " + tok,
             "Accept": "application/vnd.oci.image.index.v1+json,"
                       " application/vnd.docker.distribution.manifest.list.v2+json"})
try:
    r = urllib.request.urlopen(req, timeout=15)
    d = json.load(r)
    print(f"HTTP {r.status} - PUBLIC")
    print("architectures:", [m["platform"].get("architecture") for m in d.get("manifests", [])])
except urllib.error.HTTPError as e:
    print(f"HTTP {e.code} {e.reason} - NOT PUBLIC YET")
