from src.vulcano_telemetry.exceptions import (
    VulcanoError,
    SQSConnectionError,
    WebhookDeliveryError,
    WebhookSignatureError,
)


def test_excepcion_base():
    error = VulcanoError(
        "Error de prueba",
        details={"payment_id": "PAY-123", "status": "failed"},
    )

    assert str(error) == "Error de prueba"
    assert error.details == {
        "payment_id": "PAY-123",
        "status": "failed",
    }


def test_detalles_por_defecto():
    error = VulcanoError("Error sin detalles")

    assert error.details == {}


def test_excepcion_hija():
    error = SQSConnectionError(
        "No se pudo conectar con SQS",
        details={"queue": "payments"},
    )

    assert str(error) == "No se pudo conectar con SQS"
    assert error.details == {"queue": "payments"}
    assert isinstance(error, VulcanoError)


def test_otra_excepcion_hija():
    error = WebhookSignatureError(
        "Firma inválida",
        details={"payment_id": "PAY-456"},
    )

    assert str(error) == "Firma inválida"
    assert error.details["payment_id"] == "PAY-456"
