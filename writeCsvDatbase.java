import java.io.{File, FileOutputStream, PrintWriter}
import java.sql.{Connection, DriverManager}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.io.Source
import java.io.FileWriter
import scala.collection.mutable.ArrayBuffer
import scala.math.BigDecimal.double2bigDecimal
import scala.math.round
import scala.math.round



object Main extends App {

  /* --------------------------------- Logger Functions -------------------------------- */

  def logEvent(writer: PrintWriter, logLevel: String, message: String): Unit = {
    val timestamp = java.time.LocalDateTime.now()
    val formattedTimestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(timestamp)
    writer.write(s"Timestamp: $formattedTimestamp\tLogLevel: $logLevel\tMessage: $message\n")
    writer.flush()
  }

  /* --------------------------------- Database Connection Functions -------------------------------- */

  def connectToDatabase(): Connection = {
    val url = "jdbc:oracle:thin:@//localhost:1521/XE"
    val username = "SCALA"
    val password = "scala"
    DriverManager.getConnection(url, username, password)
  }

  def writeToDatabase(order: Order, discount: Double, finalPrice: Double, connection: Connection): Unit = {
    val insertStatement =
      """
        |INSERT INTO SCALA.discount (order_date, product_name, expiry_date, quantity, channel, payment_method, unit_price, discount, final_price)
        |VALUES (?, ?, ?, ?, ?, ?, ?, ? , ?)
        |""".stripMargin

    val preparedStatement = connection.prepareStatement(insertStatement)
    preparedStatement.setTimestamp(1, java.sql.Timestamp.valueOf(order.timestamp.atStartOfDay))
    preparedStatement.setString(2, order.productName)
    preparedStatement.setDate(3, java.sql.Date.valueOf(order.expiryDate))
    preparedStatement.setInt(4, order.quantity)
    preparedStatement.setString(5, order.channel)
    preparedStatement.setString(6, order.paymentMethod)

    preparedStatement.setDouble(7, order.unitPrice)
    val roundedDiscount = BigDecimal(discount * 100).setScale(1, BigDecimal.RoundingMode.HALF_UP)
    preparedStatement.setString(8, s"$roundedDiscount%")
    preparedStatement.setDouble(9, finalPrice)

    preparedStatement.executeUpdate()
    preparedStatement.close()
  }

  /* ------------------------------ Qualification Functions with logs ------------------------------ */

  def qualify_23March(order: Order): Boolean = {
    val qualifyResult = order.timestamp.getDayOfMonth == 23 && order.timestamp.getMonthValue == 3
    logEvent(logWriter, "Info", s"Qualify: Order qualifies for 23 March: $qualifyResult")
    qualifyResult
  }

  def qualify_expiryDays(order: Order): Boolean = {
    val daysToExpiry = java.time.temporal.ChronoUnit.DAYS.between(order.timestamp, order.expiryDate)
    val qualifyResult = daysToExpiry < 30
    logEvent(logWriter, "Info", s"Qualify: Order qualifies as the product has less than 30 days to expire: $qualifyResult")
    qualifyResult
  }

  def qualify_category(order: Order): Boolean = {
    val name = order.productName.toLowerCase
    val qualifyResult = name.contains("wine") || name.contains("cheese")
    logEvent(logWriter, "Info", s"Qualify: Order qualifies based on category: $qualifyResult")
    qualifyResult
  }

  def qualify_visa(order: Order): Boolean = {
    val qualifyResult = order.paymentMethod.equalsIgnoreCase("Visa")
    logEvent(logWriter, "Info", s"Qualify: Order qualifies based on payment method (Visa): $qualifyResult")
    qualifyResult
  }

  def qualify_quantity(order: Order): Boolean = {
    val qualifyResult = order.quantity > 5
    logEvent(logWriter, "Info", s"Qualify: Order qualifies based on quantity sold, quantity sold is more than 5: $qualifyResult")
    qualifyResult
  }

  def qualify_app(order: Order): Boolean = {
    val qualifyResult = order.channel.equalsIgnoreCase("App")
    logEvent(logWriter, "Info", s"Qualifying condition: Order qualifies for app sales: $qualifyResult")
    qualifyResult
  }

  /* --------------------------------- Discount Calculation Functions -------------------------------- */

  def calculate_23March_discount(order: Order): Double = {
    if (qualify_23March(order)) 0.5 else 0.0
  }

  def calculate_expiryDays_discount(order: Order): Double = {
    val daysToExpiry = java.time.temporal.ChronoUnit.DAYS.between(order.timestamp, order.expiryDate)
    if (qualify_expiryDays(order)) (30.0 - daysToExpiry) / 100.0 else 0.0
  }

  def calculate_category_discount(order: Order): Double = {
    val name = order.productName.toLowerCase
    if (name.contains("wine")) 0.05
    else if (name.contains("cheese")) 0.1
    else 0.0
  }

  def calculate_visa_discount(order: Order): Double = {
    if (qualify_visa(order)) 0.05 else 0.0
  }

  def calculate_quantity_discount(order: Order): Double = {
    val quantity = order.quantity
    if (quantity >= 6 && quantity <= 9) 0.05
    else if (quantity >= 10 && quantity <= 14) 0.07
    else if (quantity >= 15) 0.1
    else 0.0
  }

  def calculate_app_discount(order: Order): Double = {
    val roundedQuantity = math.ceil(order.quantity / 5.0) * 5
    if (roundedQuantity <= 5) 0.05
    else if (roundedQuantity <= 10) 0.1
    else if (roundedQuantity <= 15) 0.15
    else 0.20
  }

  /* --------------------------------- end of small functions  -------------------------------- */
  /* --------------------------------- Process Orders -------------------------------- */

  case class Order(timestamp: LocalDate, productName: String, expiryDate: LocalDate, quantity: Int, unitPrice: Double, channel: String, paymentMethod: String)
  // Create/open the log file for writing
  val logWriter = new PrintWriter(new FileOutputStream("C:/Users/hp/IdeaProjects/res/logs.log", true))
  logEvent(logWriter, "Info", "Program Started")

  logEvent(logWriter, "Info", "Start reading CSV file")
  // Read  from the CSV
  val orders = Source.fromFile("C:/Users/hp/IdeaProjects/res/TRX1000.csv").getLines().toList.tail
  logEvent(logWriter, "Info", "Finished reading CSV file")

  val connection = connectToDatabase()
  /* -----------------------------------  Function Pairs List -------------------------------------------*/
  //we can add any new role to this map
  val qualify_discount_map: List[(Order => Boolean, Order => Double, String)] = List(
    (qualify_quantity, calculate_quantity_discount, "Quantity Discount"),
    (qualify_category, calculate_category_discount, "Category Discount"),
    (qualify_23March, calculate_23March_discount, "23 March Discount"),
    (qualify_expiryDays, calculate_expiryDays_discount, "Expiry Days Discount"),
    (qualify_app, calculate_app_discount, "App Discount"),
    (qualify_visa, calculate_visa_discount, "Visa Discount")
  )
  /* ---------------------------------- -------------------------------------------*/
  //  processing each order individually,
  // the code can accurately apply the discount rules and calculate the final price for each order
  // Define a CSV writer
  val csvWriter = new FileWriter(new File("C:/Users/hp/IdeaProjects/res/orders_with_discounts.csv"))
  // Write CSV header
  csvWriter.write("Timestamp,Product Name,Expiry Date,Quantity,Unit Price,Channel,Payment Method,Discount,Final Price\n")

  orders.foreach { orderString =>
    val orderParts = orderString.split(",")
    val orderDate = LocalDate.parse(orderParts(0), DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))
    val expiryDate = LocalDate.parse(orderParts(2))
    val order = Order(
      timestamp = orderDate,
      productName = orderParts(1),
      expiryDate = expiryDate,
      quantity = orderParts(3).toInt,
      unitPrice = orderParts(4).toDouble,
      channel = orderParts(5),
      paymentMethod = orderParts(6)
    )

    //callculte the discount using qualify_discount_map function
    val discounts = qualify_discount_map.flatMap { case (qualify, calculate, ruleName) =>
      if (qualify(order)) Some((calculate(order), ruleName)) else None
    }


    // Get the top two discounts from the discounts val
    val topTwoDiscounts = discounts.sortBy(-_._1).take(2)
    val finalDiscount = if (topTwoDiscounts.nonEmpty) topTwoDiscounts.map(_._1).sum / topTwoDiscounts.length else 0.0
    val finalPrice = order.unitPrice * order.quantity * (1 - finalDiscount)


    // Write to CSV
    val discountString = (finalDiscount.setScale(3, BigDecimal.RoundingMode.HALF_UP))*100
    csvWriter.write(s"${order.timestamp},${order.productName},${order.expiryDate},${order.quantity},${order.unitPrice},${order.channel},${order.paymentMethod},$discountString,$finalPrice\n")

    writeToDatabase(order, finalDiscount, finalPrice, connection)
    // to write to the logs what roles cause the discount
    topTwoDiscounts.foreach { case (discount, ruleName) =>
      logEvent(logWriter, "Info", s"Discount calculated using rule: $ruleName, Discount: $discount")
    }
  }
  logWriter.close()
  connection.close()
  csvWriter.close()}
