import org.apache.spark.sql.Row


val row = Row(350, true, "Spork!", null)

println(row.getInt(0))
println(row.getBoolean(1))
println(row.getString(2))


case class DeviceIoTData(
  battery_level: Long, 
  c02_level: Long,
  cca2: String,
  cca3: String,
  cn: String,
  device_id: Long,
  device_name: String,
  humidity: Long,
  ip: String,
  latitude: Double,
  lcd: String,
  longitude: Double,
  scale: String,
  temp: Long,
  timestamp: Long)

val ds = spark
  .read
  .json(s"$DATA/databricks-datasets/learning-spark-v2/iot-devices/iot_devices.json")
  .as[DeviceIoTData]
ds.show(5, false)