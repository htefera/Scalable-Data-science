package A;

public class Utils{

    private static final String[] orderAttributes = new String[] {
            "orderkey","custkey","orderstatus","price","orderdate","orderpriority","clerk",
            "shippriority","comment"
    };


    public static String getOrdersAttribute(String[] row, String attribute) {
        for (int i = 0; i < orderAttributes.length; i++) {
            if (orderAttributes[i].equals(attribute)) {
                return row[i];
            }
        }
        return null;
    }

    private static final String[] customerAttributes = new String[] {
            "custkey","name","address","nationkey","phone","acctbal","mktsegment",
            "comment"
    };


    public static String getCustomerAttribute(String[] row, String attribute) {
        for (int i = 0; i < customerAttributes.length; i++) {
            if (customerAttributes[i].equals(attribute)) {
                return row[i];
            }
        }
        return null;
    }


}
