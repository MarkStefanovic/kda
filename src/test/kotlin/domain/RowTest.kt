package domain

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RowTest {
//    private val mongo = startMongoContainer().apply {
//        configure()
//    }
//
//    private val mongoDAO = MongoDAO(mongo.host, mongo.port)

    @Test
    fun subset() {
        val row = Row(mapOf(
            "customer_id" to IntValue(1),
            "first_name" to StringValue(value ="Mark", maxLength = 40),
            "last_name" to StringValue(value = "Stefanovic", maxLength = 40),
        ))
        val subset = row.subset("first_name", "last_name")
        val expected = Row(mapOf(
            "first_name" to StringValue(value ="Mark", maxLength = 40),
            "last_name" to StringValue(value = "Stefanovic", maxLength = 40),
        ))
        assertEquals(expected = expected, actual = subset)
    }
}