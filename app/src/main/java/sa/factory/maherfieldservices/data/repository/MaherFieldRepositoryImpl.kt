package sa.factory.maherfieldservices.data.repository

import sa.factory.maherfieldservices.domain.repository.MaherFieldRepository
import sa.factory.maherfieldservices.hardware.HardwareSensorDriver
import sa.factory.maherfieldservices.model.MaherFieldItem
import sa.factory.maherfieldservices.model.MaherFieldOrder
import sa.factory.maherfieldservices.model.MaherFieldTelemetry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch

class MaherFieldRepositoryImpl(
    private val sensorDriver: HardwareSensorDriver
) : MaherFieldRepository {

    private val _catalog = MutableStateFlow<List<MaherFieldItem>>(listOf(
        MaherFieldItem("1", "حزمة البداية السريعة", "إعداد متجر إلكتروني متكامل مع تدريب على إدارة الطلبات عبر منصات التواصل الاجتماعي", 150.0, "معتمد", "إدارة الطلبات"),
        MaherFieldItem("2", "بوابة دفع NFC متكاملة", "دمج مدفوعات مادّة Contactless EMV L2 مع نظام إدارة المخزون في الوقت الفعلي", 280.0, "فوري", "بوابة الدفع الإلكتروني"),
        MaherFieldItem("3", "تحليلات المبيعات المتقدمة", "لوحة تحكم ذكية مع خرائط حرارية لمناطق الطلب وتوقعات المخزون باستخدام GPS Dispatch", 95.0, "جديد", "تتبع التوصيل")
    ))
    override val catalogFlow: Flow<List<MaherFieldItem>> = _catalog.asStateFlow()

    private val _cart = MutableStateFlow<List<MaherFieldItem>>(emptyList())
    override val activeCart: StateFlow<List<MaherFieldItem>> = _cart.asStateFlow()

    override val telemetryFlow: Flow<MaherFieldTelemetry> = sensorDriver.observePeripheralStream()
        .map { frame ->
            MaherFieldTelemetry(
                peripheralName = frame.peripheralName,
                status = frame.connectionState,
                liveReading = frame.livePayload,
                batteryPct = 96,
                signalDbm = -54
            )
        }

    override fun addToCart(item: MaherFieldItem) {
        val current = _cart.value.toMutableList()
        current.add(item)
        _cart.value = current
    }

    override fun removeFromCart(item: MaherFieldItem) {
        val current = _cart.value.toMutableList()
        current.remove(item)
        _cart.value = current
    }

    override fun clearCart() {
        _cart.value = emptyList()
    }

    override suspend fun checkout(paymentMethod: String): MaherFieldOrder {
        val currentItems = _cart.value.toList()
        val subtotal = currentItems.sumOf { it.priceSar }
        val vat = subtotal * 0.15
        val total = subtotal + vat
        val order = MaherFieldOrder(
            orderId = "ORD-${System.currentTimeMillis() % 100000}",
            items = currentItems,
            subtotalSar = subtotal,
            vatSar = vat,
            totalSar = total,
            paymentMethod = paymentMethod
        )
        clearCart()
        return order
    }

    override suspend fun refreshCatalog() {
        // Offline-first sync logic
    }
}
