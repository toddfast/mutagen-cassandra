package com.toddfast.mutagen.cassandra.impl.info;


public class MigrationInfoCommand {
    private static final String VERSION_TITLE = "Version";

    private static final String DATE_TITLE = "Execution Date";

    private static final String FILENAME_TITLE = "Filename";

    private static final String STATE_TITLE = "State";

    /**
     * private constructor,prevent instantiation.
     */
    private MigrationInfoCommand() {

    }

    /**
     * Print all the infos about the migrations into an ascii table.
     * 
     * @param migrationInfos
     *            The list of migrations to print.
     * 
     * @return The String representation of migrations results.
     * 
     */
    public static String printInfo(MigrationInfo[] migrationInfos) {
        int versionWidth = VERSION_TITLE.length();
        int dateWidth = DATE_TITLE.length();
        int filenameWidth = FILENAME_TITLE.length();
        int stateWidth = 8;

        for (MigrationInfo migrationInfo : migrationInfos) {
            versionWidth = Math.max(versionWidth, migrationInfo.getVersion().toString().length());
            dateWidth = Math.max(dateWidth, migrationInfo.getDate().toString().length());
            filenameWidth = Math.max(filenameWidth, migrationInfo.getFilename().length());
        }

        String ruler = "+-" + trimOrPad("", versionWidth, '-')
                + "-+-" + trimOrPad("", dateWidth, '-')
                + "-+-" + trimOrPad("", filenameWidth, '-')
                + "-+-" + trimOrPad("", stateWidth, '-')
                + "-+\n";

        StringBuffer table = new StringBuffer();
        table.append(ruler);
        table.append("| ").append(trimOrPad(VERSION_TITLE, versionWidth, ' '))
                .append(" | ").append(trimOrPad(DATE_TITLE, dateWidth, ' '))
                .append(" | ").append(trimOrPad(FILENAME_TITLE, filenameWidth, ' '))
                .append(" | ").append(trimOrPad(STATE_TITLE, stateWidth, ' '))
                .append(" |\n");
        table.append(ruler);

        if (migrationInfos.length == 0) {
            table.append(trimOrPad("| No migrations found", ruler.length() - 2, ' ')).append("|\n");
        } else {
            for (MigrationInfo migrationInfo : migrationInfos) {
                table.append("| ").append(trimOrPad(migrationInfo.getVersion(), versionWidth, ' '));
                table.append(" | ").append(trimOrPad(migrationInfo.getDate().toString(), dateWidth, ' '));
                table.append(" | ").append(trimOrPad(migrationInfo.getFilename(), filenameWidth, ' '));
                table.append(" | ").append(
                        trimOrPad((migrationInfo.getSuccess() ? "success" : "failed"), stateWidth, ' '));
                table.append(" |\n");
            }
        }

        table.append(ruler);
        return table.toString();

    }

    /**
     * Trims or pads this string, so it has this exact length.
     *
     * @param str
     *            The string to adjust. {@code null} is treated as an empty string.
     * @param length
     *            The exact length to reach.
     * @param padChar
     *            The padding character.
     * @return The adjusted string.
     */
    public static String trimOrPad(String str, int length, char padChar) {
        String result;
        if (str == null) {
            result = "";
        } else {
            result = str;
        }

        if (result.length() > length) {
            return result.substring(0, length);
        }

        while (result.length() < length) {
            result += padChar;
        }
        return result;
    }
}
