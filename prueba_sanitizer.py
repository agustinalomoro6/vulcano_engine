import argparse
from src.vulcano_telemetry.sanitizer import parse_secret_key

try:
    parse_secret_key("12345678")
    print("ERROR: se aceptó una clave demasiado corta")
except argparse.ArgumentTypeError as e:
    print("SECRET INVALID REJECTED:", e)