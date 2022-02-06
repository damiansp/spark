val filterTempDS = ds.filter(d => {d.temp > 30 && d.humidity > 70})
filterTempDS.show(5, false)


case class DeviceTempByCountry(temp: Long, device_name: String, device_id: Long, cca3: String)
val dsTemp = ds 
  .filter(d => {d.temp > 25})
  .map(d => (d.temp, d.device_name, d.device_id, d.cca3))
  .toDF("temp", "device_name", "device_id", "cca3")
  .as[DeviceTempByCountry]
dsTemp.show(5, false)

val device = dsTemp.first()
println(device)

val dsTemp2 = ds 
  .select($"temp", $"device_name", $"device_id", $"cca3")
  .wehere("temp > 25")
  .as[DeviceTempByCountry]

