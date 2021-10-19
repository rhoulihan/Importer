package com.amazonaws.Importer;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;

public class Main {
	private static Connection conn = null;
	private static Map<Integer, Item> employees = new HashMap<Integer, Item>();
	private static Random random = new Random();
	private static Scanner scanner = new Scanner(System.in);

	public static String target;
	public static ThreadPoolExecutor tpe = (ThreadPoolExecutor) Executors.newFixedThreadPool(60);
	public static Object sync = new Object();
	public static volatile AtomicInteger numThreads = new AtomicInteger(0);
	public static DynamoDB db;
	public static Map<Integer, Map<String, List<String>>> results = new HashMap<Integer, Map<String, List<String>>>();
	public static Map<Integer, Integer> counts = new HashMap<Integer, Integer>();

	// the goodness starts here
	public static void main(String[] args) {
		disableWarning();

		// configure the client
		ClientConfiguration config = new ClientConfiguration().withConnectionTimeout(500)
				.withClientExecutionTimeout(20000).withRequestTimeout(1000).withSocketTimeout(1000)
				.withRetryPolicy(PredefinedRetryPolicies.getDynamoDBDefaultRetryPolicyWithCustomMaxRetries(20));

		// initialize the connection
		db = new DynamoDB(AmazonDynamoDBClientBuilder.standard().withClientConfiguration(config)
				.withCredentials(new ProfileCredentialsProvider("default")).build());

		// set the target table
		target = args[0];

		// connect the MySQL instance
		connect(args[1]);

		// Import the items from mySQL
		if (Boolean.valueOf(args[2])) {
			migrate("employees");
			migrate("departments");
			migrate("dept_manager");
			migrate("dept_emp");
			migrate("titles");
			migrate("salaries");

			System.out.println("Import complete, press [ENTER] to continue.");
			scanner.nextLine();
		}

		// Run the queries against both backends, MySQL first
		System.out.println();
		System.out.println("Executing MySQL queries:");
		mysqlQuery(
				"select * from employees inner join dept_emp on employees.emp_no = dept_emp.emp_no where to_date = '9999-01-01' and dept_no = 'd005' order by last_name");

		System.out.println();
		mysqlQuery(
				"select * from employees inner join dept_emp on employees.emp_no = dept_emp.emp_no inner join titles on employees.emp_no = titles.emp_no inner join salaries on employees.emp_no = salaries.emp_no where employees.emp_no = 88958 and titles.to_date = '9999-01-01' and dept_emp.to_date = '9999-01-01' and salaries.to_date = '9999-01-01'");

		System.out.println();
		mysqlQuery(
				"select * from employees inner join dept_manager on employees.emp_no = dept_manager.emp_no where to_date = '9999-01-01' and dept_no = 'd002';");

		System.out.println();
		mysqlQuery(
				"select * from employees inner join dept_emp on employees.emp_no = dept_emp.emp_no inner join titles on employees.emp_no = titles.emp_no where dept_emp.dept_no = 'd002' and titles.title = 'Senior Staff' and dept_emp.to_date = '9999-01-01' and titles.to_date = '9999-01-01'");

		// now DynamoDB
		System.out.println();
		System.out.println("Executing DynamoDB queries:");
		QuerySpec spec = new QuerySpec().withKeyConditionExpression("PK1 = :dept");
		dynamoQuery(spec, true, 25, "d005");

		System.out.println();
		spec = new QuerySpec().withKeyConditionExpression("PK = :emp_no and SK > :val")
				.withValueMap(new ValueMap().withString(":emp_no", "88958").withString(":val", "9"));
		dynamoQuery(spec, false, 1, null);

		System.out.println();
		spec = new QuerySpec().withKeyConditionExpression("PK1 = :dept_no and begins_with(SK1, :val)")
				.withValueMap(new ValueMap().withString(":dept_no", "d002").withString(":val", "M#"));
		dynamoQuery(spec, true, 1, null);

		System.out.println();
		spec = new QuerySpec().withKeyConditionExpression("PK1 = :dept")
				.withValueMap(new ValueMap().withString(":dept_title", "d002#Senior Staff"));
		dynamoQuery(spec, true, 2, "d002#Senior Staff");

		// say goodbye
		try {
			tpe.shutdown();
			conn.close();
			scanner.close();
		} catch (Exception e) {
			System.err.println(String.format("ERROR: %s", e.getMessage()));
			System.exit(1);
		}
	}

	// wait for threads to complete
	private static void waitForWorkers() {
		while (numThreads.get() > 0)
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				System.err.println(String.format("ERROR: %s", e.getMessage()));
			}
	}

	// connect to MySQL
	private static void connect(String hostname) {
		System.out.println(String.format("Connecting to mysql instance on host [%s]...", hostname));
		String dbURL = String.format("jdbc:mysql://%s:3306/employees", hostname);
		String username = "root";
		String password = "password";

		try {

			conn = DriverManager.getConnection(dbURL, username, password);

			if (conn == null) {
				System.err.println(String.format("ERROR: Unable to connect to host (%s)", hostname));
				System.exit(1);
			}
		} catch (SQLException ex) {
			System.err.println(String.format("ERROR: %s", ex.getMessage()));
			System.exit(1);
		}

		System.out.println("Connected.");
	}

	// run a mysql query
	private static void mysqlQuery(String sql) {
		try {
			int count = 0;
			long elapsed = System.currentTimeMillis();
			Statement statement = conn.createStatement();
			ResultSet result = statement.executeQuery(sql);

			while (result.next()) {
				count++;
			}

			System.out.println(String.format("Executed mySQL query: \n%s\nRetrieved %d items in %dms.", sql, count,
					System.currentTimeMillis() - elapsed));
		} catch (SQLException e) {
			System.err.println(String.format("ERROR: %s", e.getMessage()));
			System.exit(1);
		}
	}

	// run a dynamodb query
	private static void dynamoQuery(QuerySpec spec, boolean gsiQuery, int workers, String pk) {
		long elapsed = System.currentTimeMillis();
		counts.clear();

		if (workers > 1)
			for (int i = 0; i < workers; i++)
				tpe.execute(new RunQuery(Integer.valueOf(i),
						spec.withValueMap(new ValueMap().withString(":dept", String.format("%s#%d", pk, i))),
						gsiQuery));
		else
			tpe.execute(new RunQuery(Integer.valueOf(0), spec, gsiQuery));

		waitForWorkers();

		int count = 0;
		for (Integer key : counts.keySet()) {
			count += counts.get(key);
		}

		System.out.println(String.format("Executed DynamoDB query:\n%s\nRetrieved %d items in %dms.",
				spec.getKeyConditionExpression(), count, System.currentTimeMillis() - elapsed));
	}

	// Move the data from MySQL into DynamoDB
	private static void migrate(String table) {
		System.out.print(String.format("Migrating table [%s]...", table));
		long elapsed = System.currentTimeMillis();

		String sql = String.format("SELECT * FROM %s", table);

		int count = 0;

		try {
			Statement statement = conn.createStatement();
			ResultSet result = statement.executeQuery(sql);
			Map<String, String> map = new HashMap<String, String>();

			TableWriteItems items = new TableWriteItems(target);
			Item item;

			while (result.next()) {
				item = new Item();
				switch (table) {

				// create employee items keyed on emp_no with a generic sort key
				case "employees":
					item.withString("PK", Integer.toString(result.getInt("emp_no")));
					item.withString("SK", "A");
					item.withString("birth_date", result.getDate("birth_date").toString());
					item.withString("first_name", result.getString("first_name"));
					item.withString("last_name", result.getString("last_name"));
					item.withString("gender", result.getString("gender"));
					item.withString("hire_date", result.getDate("hire_date").toString());
					employees.put(result.getInt("emp_no"), item);
					break;

				// create the department items...only a few of them
				case "departments":
					item.withString("PK", result.getString("dept_no"));
					item.withString("SK", result.getString("dept_name"));
					item.withString("PK1", result.getString("dept_no"));
					item.withString("SK1", result.getString("dept_name"));
					break;

				// same for managers, not very many
				case "dept_manager":
					item.withString("PK", Integer.toString(result.getInt("emp_no")));
					item.withString("SK",
							String.format("%s#%s", result.getDate("to_date").toString(), result.getString("dept_no")));
					item.withString("from_date", result.getDate("from_date").toString());

					// if this is a current manager then index the item on the dept_no with a
					// prefixed sort key and denormalize the employee name
					if (result.getDate("to_date").toString().equals("9999-01-01")) {
						Item employee = employees.get(Integer.valueOf(result.getInt("emp_no")));
						item.withString("PK1", result.getString("dept_no"));
						item.withString("SK1", String.format("M#%s, %s", employee.getString("last_name"),
								employee.getString("first_name")));
					}
					break;

				// load the department employee records
				case "dept_emp":
					item.withString("PK", Integer.toString(result.getInt("emp_no")));
					item.withString("SK", String.format("%s#D", result.getDate("to_date").toString()));
					item.withString("from_date", result.getDate("from_date").toString());
					item.withString("to_date", result.getDate("to_date").toString());

					// deal with dirty data issues (duplicate keys)
					if (map.putIfAbsent(String.format("%d#%s", result.getInt("emp_no"), item.getString("SK")),
							"T") != null)
						item.withString("SK", "9999-01-01#D");

					// if this is a current team member then index on dept_no and sort on full name.
					// write shard the GSI since we are bulk loading and creating artificial key
					// pressure and denormalize employee full name for the SK
					if (item.getString("SK").equals("9999-01-01#D")) {
						Item employee = employees.get(Integer.valueOf(result.getInt("emp_no")));
						item.withString("PK1", String.format("%s#%d", result.getString("dept_no"), random.nextInt(25)));
						item.withString("SK1", String.format("%s, %s", employee.getString("last_name"),
								employee.getString("first_name")));
						employee.withString("dept_no", result.getString("dept_no"));
					}
					break;

				// load the job history
				case "titles":
					item.withString("PK", Integer.toString(result.getInt("emp_no")));
					item.withString("SK", String.format("%s#T", result.getDate("to_date").toString()));
					item.withString("from_date", result.getDate("from_date").toString());
					item.withString("title", result.getString("title"));

					// deal with dirty data issues
					if (map.putIfAbsent(String.format("%d#%s", result.getInt("emp_no"), item.getString("SK")),
							"T") != null)
						item.withString("SK", "9999-01-01#T");

					if (item.getString("SK").equals("9999-01-01#T")) {
						Item employee = employees.get(Integer.valueOf(result.getInt("emp_no")));

						item.withString("PK1", String.format("%s#%s#%d", employee.getString("dept_no"),
								result.getString("title"), random.nextInt(2)));
						item.withString("SK1", String.format("%s, %s", employee.getString("last_name"),
								employee.getString("first_name")));
					}
					break;

				// load the salary history
				case "salaries":
					item.withString("PK", Integer.toString(result.getInt("emp_no")));
					item.withString("SK", String.format("%s#S", result.getDate("to_date").toString()));
					item.withString("from_date", result.getDate("from_date").toString());
					item.withInt("salary", result.getInt("salary"));

					// deal with dirty data issues
					if (map.putIfAbsent(String.format("%d#%s", result.getInt("emp_no"), item.getString("SK")),
							"T") != null)
						item.withString("SK", "9999-01-01#S");

					break;
				}

				items.addItemToPut(item);

				// if the batch write is full then send it
				if (items.getItemsToPut().size() == 25) {
					tpe.execute(new BatchLoad(items));
					items = new TableWriteItems(target);
				}

				count++;

				// let the user know we are still alive if its a large load
				if (count % 25000 == 0)
					System.out.print(".");
			}

			// send any items that are left
			if (items.getItemsToPut() != null) {
				tpe.execute(new BatchLoad(items));
			}

			// wait until all the writes are done
			while (numThreads.get() > 0) {
				System.out.print(".");
				Thread.sleep(100);
			}

			System.out.println(
					String.format("\nImported %d items in %dms.", count, System.currentTimeMillis() - elapsed));
		} catch (Exception e) {
			System.err.println(String.format("ERROR: %s", e.getMessage()));
			System.exit(1);
		}
	}

	private static void disableWarning() {
		try {
			Field theUnsafe = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			theUnsafe.setAccessible(true);
			sun.misc.Unsafe u = (sun.misc.Unsafe) theUnsafe.get(null);

			Class cls = Class.forName("jdk.internal.module.IllegalAccessLogger");
			Field logger = cls.getDeclaredField("logger");
			u.putObjectVolatile(cls, u.staticFieldOffset(logger), null);
		} catch (Exception e) {
			// ignore
		}
	}
}
