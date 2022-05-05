package tdsql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.NumberFormat;
import java.util.LinkedList;
import java.util.List;

public class ExecuteAndExecuteUpdate {

    public static Connection getConn() {
        return getConn("");
    }

    public static Connection getConn(String props) {
        Connection conn = null;
        try {
            Class.forName("com.tencentcloud.tdsql.mysql.cj.jdbc.Driver");

            String proxyUrl = "jdbc:tdsql-mysql://9.135.1.199:15013/dorianzhang";
            if (props != null && !"".equals(props.trim())) {
                proxyUrl += "?" + props;
            }
            try {
                conn = DriverManager.getConnection(proxyUrl, "test", "test");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return conn;
    }

    public static void main(String[] args) throws SQLException {
        Execute.case01(100);
//        ExecuteUpdate.case01(100);
    }

    public static class Execute {

        private static void case01(int nCount) throws SQLException {
            StopWatch stopWatch = new StopWatch();
            Connection conn = getConn();
            stopWatch.start("Execute.case01");
            for (int i = 0; i < nCount; i++) {
                Statement stmt = conn.createStatement();
                stmt.execute("update test_2 set username = '" + nCount + "' where id = 1");
                stmt.close();
            }
            stopWatch.stop();
            conn.close();
            System.out.println(stopWatch.prettyPrint());
        }
    }

    public static class ExecuteUpdate {

        private static void case01(int nCount) throws SQLException {
            StopWatch stopWatch = new StopWatch();
            Connection conn = getConn();
            stopWatch.start("ExecuteUpdate.case01");
            for (int i = 0; i < nCount; i++) {
                Statement stmt = conn.createStatement();
                stmt.executeUpdate("update test_2 set username = '" + nCount + "' where id = 1");
                stmt.close();
            }
            stopWatch.stop();
            conn.close();
            System.out.println(stopWatch.prettyPrint());
        }
    }

    private static class StopWatch {

        /**
         * Identifier of this stop watch.
         * Handy when we have output from multiple stop watches
         * and need to distinguish between them in log or console output.
         */
        private final String id;

        private boolean keepTaskList = true;

        private final List<TaskInfo> taskList = new LinkedList<TaskInfo>();

        /**
         * Start time of the current task
         */
        private long startTimeMillis;

        /**
         * Is the stop watch currently running?
         */
        private boolean running;

        /**
         * Name of the current task
         */
        private String currentTaskName;

        private TaskInfo lastTaskInfo;

        private int taskCount;

        /**
         * Total running time
         */
        private long totalTimeMillis;


        /**
         * Construct a new stop watch. Does not start any task.
         */
        public StopWatch() {
            this("");
        }

        /**
         * Construct a new stop watch with the given id.
         * Does not start any task.
         *
         * @param id identifier for this stop watch.
         *         Handy when we have output from multiple stop watches
         *         and need to distinguish between them.
         */
        public StopWatch(String id) {
            this.id = id;
        }


        /**
         * Return the id of this stop watch, as specified on construction.
         *
         * @return the id (empty String by default)
         * @see #StopWatch(String)
         * @since 4.2.2
         */
        public String getId() {
            return this.id;
        }

        /**
         * Determine whether the TaskInfo array is built over time. Set this to
         * "false" when using a StopWatch for millions of intervals, or the task
         * info structure will consume excessive memory. Default is "true".
         */
        public void setKeepTaskList(boolean keepTaskList) {
            this.keepTaskList = keepTaskList;
        }


        /**
         * Start an unnamed task. The results are undefined if {@link #stop()}
         * or timing methods are called without invoking this method.
         *
         * @see #stop()
         */
        public void start() throws IllegalStateException {
            start("");
        }

        /**
         * Start a named task. The results are undefined if {@link #stop()}
         * or timing methods are called without invoking this method.
         *
         * @param taskName the name of the task to start
         * @see #stop()
         */
        public void start(String taskName) throws IllegalStateException {
            if (this.running) {
                throw new IllegalStateException("Can't start StopWatch: it's already running");
            }
            this.running = true;
            this.currentTaskName = taskName;
            this.startTimeMillis = System.currentTimeMillis();
        }

        /**
         * Stop the current task. The results are undefined if timing
         * methods are called without invoking at least one pair
         * {@code start()} / {@code stop()} methods.
         *
         * @see #start()
         */
        public void stop() throws IllegalStateException {
            if (!this.running) {
                throw new IllegalStateException("Can't stop StopWatch: it's not running");
            }
            long lastTime = System.currentTimeMillis() - this.startTimeMillis;
            this.totalTimeMillis += lastTime;
            this.lastTaskInfo = new TaskInfo(this.currentTaskName, lastTime);
            if (this.keepTaskList) {
                this.taskList.add(lastTaskInfo);
            }
            ++this.taskCount;
            this.running = false;
            this.currentTaskName = null;
        }

        /**
         * Return whether the stop watch is currently running.
         *
         * @see #currentTaskName()
         */
        public boolean isRunning() {
            return this.running;
        }

        /**
         * Return the name of the currently running task, if any.
         *
         * @see #isRunning()
         * @since 4.2.2
         */
        public String currentTaskName() {
            return this.currentTaskName;
        }


        /**
         * Return the time taken by the last task.
         */
        public long getLastTaskTimeMillis() throws IllegalStateException {
            if (this.lastTaskInfo == null) {
                throw new IllegalStateException("No tasks run: can't get last task interval");
            }
            return this.lastTaskInfo.getTimeMillis();
        }

        /**
         * Return the name of the last task.
         */
        public String getLastTaskName() throws IllegalStateException {
            if (this.lastTaskInfo == null) {
                throw new IllegalStateException("No tasks run: can't get last task name");
            }
            return this.lastTaskInfo.getTaskName();
        }

        /**
         * Return the last task as a TaskInfo object.
         */
        public TaskInfo getLastTaskInfo() throws IllegalStateException {
            if (this.lastTaskInfo == null) {
                throw new IllegalStateException("No tasks run: can't get last task info");
            }
            return this.lastTaskInfo;
        }


        /**
         * Return the total time in milliseconds for all tasks.
         */
        public long getTotalTimeMillis() {
            return this.totalTimeMillis;
        }

        /**
         * Return the total time in seconds for all tasks.
         */
        public double getTotalTimeSeconds() {
            return this.totalTimeMillis / 1000.0;
        }

        /**
         * Return the number of tasks timed.
         */
        public int getTaskCount() {
            return this.taskCount;
        }

        /**
         * Return an array of the data for tasks performed.
         */
        public TaskInfo[] getTaskInfo() {
            if (!this.keepTaskList) {
                throw new UnsupportedOperationException("Task info is not being kept!");
            }
            return this.taskList.toArray(new TaskInfo[this.taskList.size()]);
        }


        /**
         * Return a short description of the total running time.
         */
        public String shortSummary() {
            return "StopWatch '" + getId() + "': running time (millis) = " + getTotalTimeMillis();
        }

        /**
         * Return a string with a table describing all tasks performed.
         * For custom reporting, call getTaskInfo() and use the task info directly.
         */
        public String prettyPrint() {
            StringBuilder sb = new StringBuilder(shortSummary());
            sb.append('\n');
            if (!this.keepTaskList) {
                sb.append("No task info kept");
            } else {
                sb.append("-----------------------------------------\n");
                sb.append("ms     %     Task name\n");
                sb.append("-----------------------------------------\n");
                NumberFormat nf = NumberFormat.getNumberInstance();
                nf.setMinimumIntegerDigits(5);
                nf.setGroupingUsed(false);
                NumberFormat pf = NumberFormat.getPercentInstance();
                pf.setMinimumIntegerDigits(3);
                pf.setGroupingUsed(false);
                for (TaskInfo task : getTaskInfo()) {
                    sb.append(nf.format(task.getTimeMillis())).append("  ");
                    sb.append(pf.format(task.getTimeSeconds() / getTotalTimeSeconds())).append("  ");
                    sb.append(task.getTaskName()).append("\n");
                }
            }
            return sb.toString();
        }

        /**
         * Return an informative string describing all tasks performed
         * For custom reporting, call {@code getTaskInfo()} and use the task info directly.
         */
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(shortSummary());
            if (this.keepTaskList) {
                for (TaskInfo task : getTaskInfo()) {
                    sb.append("; [").append(task.getTaskName()).append("] took ").append(task.getTimeMillis());
                    long percent = Math.round((100.0 * task.getTimeSeconds()) / getTotalTimeSeconds());
                    sb.append(" = ").append(percent).append("%");
                }
            } else {
                sb.append("; no task info kept");
            }
            return sb.toString();
        }


        /**
         * Inner class to hold data about one task executed within the stop watch.
         */
        public static final class TaskInfo {

            private final String taskName;

            private final long timeMillis;

            TaskInfo(String taskName, long timeMillis) {
                this.taskName = taskName;
                this.timeMillis = timeMillis;
            }

            /**
             * Return the name of this task.
             */
            public String getTaskName() {
                return this.taskName;
            }

            /**
             * Return the time in milliseconds this task took.
             */
            public long getTimeMillis() {
                return this.timeMillis;
            }

            /**
             * Return the time in seconds this task took.
             */
            public double getTimeSeconds() {
                return (this.timeMillis / 1000.0);
            }
        }

    }
}
