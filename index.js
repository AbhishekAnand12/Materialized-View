import { MongoClient } from "mongodb";

const uri = "mongodb://localhost:27017";
const dbName = "materialized-View";
const materializedView = async () => {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    const db = client.db(dbName);
    const orderscollection = db.collection("orders");

    await orderscollection.insertMany([
      { orderId: 1, customerId: 1, amount: 100, date: new Date("2022-01-01") },
      { orderId: 2, customerId: 1, amount: 200, date: new Date("2022-01-02") },
      { orderId: 3, customerId: 2, amount: 300, date: new Date("2022-01-03") },
      { orderId: 4, customerId: 2, amount: 400, date: new Date("2022-01-04") },
    ]);

    const customersCollection = client.db(dbName).collection("customers");
    await customersCollection.insertMany([
      { customerId: 1, name: "Alice" },
      { customerId: 2, name: "Bob" },
    ]);

    await db
      // .db(dbName)
      .createCollection("monthlyRevenue", {
        viewOn: "orders",
        pipeline: [
          {
            $lookup: {
              from: "customers",
              localField: "customerId",
              foreignField: "customerId",
              as: "customer",
            },
          },
          {
            $unwind: "$customer",
          },
          {
            $group: {
              _id: {
                year: { $year: "$date" },
                month: { $month: "$date" },
                customerId: "$customerId",
                name: "$customer.name",
              },
              revenue: { $sum: "$amount" },
            },
          },
          {
            $project: {
              _id: 0,
              year: "$_id.year",
              month: "$_id.month",
              customerId: "$_id.customerId",
              name: "$_id.name",
              revenue: "$revenue",
            },
          },
          {
            $merge: {
              into: { db: dbName, coll: "monthlyRevenue" },
              on: ["year", "month", "customerId"],
              whenMatched: "replace",
              whenNotMatched: "insert",
            },
          },
        ],
      });

    console.log("Materialized view created successfully");

    // Query the view
    // const view = db.collection("monthlyRevenue");
    // const result = await view.find().toArray();
    // console.log(result);

  } catch (err) {

    console.log("error in catch::->", err.message);
    await client.close();
    
  }
};

materializedView();
