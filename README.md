# The Scala Discounts Rule Engine 
Was created to streamline discount computations for a retail establishment by leveraging a series of predefined rules. 
hese rules are tailored to ascertain eligibility for discounts based on specific qualifying criteria, such as product type, quantity, expiration date, and special dates.
This application interacts with an Oracle database, facilitated by the Oracle JDBC driver, to seamlessly integrate processed order data for further analysis and reporting. 
Detailed logs are generated to provide insights into rule interactions and errors, aiding in debugging and auditing efforts.

# Implemented Discount Rules include:
**More Than 5 Qualifier Rule:**
Identifies orders with quantities exceeding 5 units and applies tiered discounts of 5%, 7%, or 10% accordingly.

**Cheese and Wine Qualifier Rule:**
Detects orders containing wine or cheese products and applies discounts of 5% for wine and 10% for cheese.

**Less Than 30 Days to Expiry Qualifier Rule:**
Determines if products in the order have less than 30 days remaining before expiration. Discounts start from 1% and increment by 1% per day, up to a maximum of 30%.

**Products Sold on 23rd of March Qualifier Rule:**
Identifies orders made on the 23rd of March and offers a special 50% discount.

**App Usage Qualifier Rule:** 
Offers discounts of 5%, 10%, or 15% based on the quantity of units sold through the App.

**Visa Card Usage Qualifier Rule:**
Applies a flat 5% discount for orders paid with Visa cards.

# want to use ?

Clone Repository: Clone the repository to your local machine.<https://github.com/malakSherif86/RuleEngineUsingScala/new/master?filename=README.md>

Import Project: Import the project into your preferred Scala development environment.

Database Configuration:

Update database connection details within the Singleton object in the code, including Oracle database URL, username, and password.

Review logs.txt: for detailed event logs and any encountered errors during processing.

# want to write to csv 
YOU can add those liness
// Define a CSV writer
  val csvWriter = new FileWriter(new File("C:/Users/hp/IdeaProjects/res/orders_with_discounts.csv"))
  // Write CSV header
  csvWriter.write("Timestamp,Product Name,Expiry Date,Quantity,Unit Price,Channel,Payment Method,Discount,Final Price\n")
val discountString = (finalDiscount.setScale(3, BigDecimal.RoundingMode.HALF_UP))*100
    csvWriter.write(s"${order.timestamp},${order.productName},${order.expiryDate},${order.quantity},${order.unitPrice},${order.channel},${order.paymentMethod},$discountString,$finalPrice\n")
    csvWriter.close()
**i also provided the code that can write both the to database and the csv in the files called writeCsvDatbase**
