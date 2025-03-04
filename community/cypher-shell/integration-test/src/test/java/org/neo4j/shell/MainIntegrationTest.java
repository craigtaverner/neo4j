/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.shell;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.shell.DatabaseManager.DEFAULT_DEFAULT_DB_NAME;
import static org.neo4j.shell.DatabaseManager.SYSTEM_DB_NAME;
import static org.neo4j.shell.terminal.CypherShellTerminalBuilder.terminalBuilder;
import static org.neo4j.shell.test.Util.testConnectionConfig;
import static org.neo4j.shell.util.Versions.majorVersion;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.function.ThrowingAction;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.shell.cli.Format;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.log.Logger;
import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.parser.StatementParser.CypherStatement;
import org.neo4j.shell.prettyprint.PrettyConfig;
import org.neo4j.shell.prettyprint.PrettyPrinter;
import org.neo4j.shell.printer.AnsiPrinter;
import org.neo4j.shell.state.BoltStateHandler;
import org.neo4j.shell.terminal.TestSimplePrompt;
import org.neo4j.shell.test.AssertableMain;
import org.neo4j.shell.util.Version;
import org.neo4j.shell.util.Versions;

// NOTE! Consider adding tests to BinaryNonInteractiveIntegrationTest instead of here.
@Timeout(value = 5, unit = MINUTES)
class MainIntegrationTest {
    private static final String USER = "neo4j";
    private static final String PASSWORD = "neo";
    private static final String newLine = System.lineSeparator();
    private static final String GOOD_BYE = format(":exit%n%nBye!%n");

    private final Version serverVersion = Versions.version(runInDbAndReturn("", CypherShell::getServerVersion));
    private final Matcher<String> endsWithInteractiveExit = endsWith(format("> %s", GOOD_BYE));

    private Matcher<String> returned42AndExited() {
        return Matchers.allOf(containsString(return42Output()), endsWithInteractiveExit);
    }

    @Test
    void promptsOnWrongAuthenticationIfInteractive() throws Exception {
        testWithUser("kate", "bush", false)
                .args("--format verbose")
                .userInputLines("kate", "bush", "return 42 as x;", ":exit")
                .run()
                .assertSuccess()
                .assertThatOutput(startsWith(format("username: kate%npassword: %n")), returned42AndExited());
    }

    @Test
    void shouldRunShowUsers() throws Exception {
        assumeAtLeastVersion("4.4.0");
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines("SHOW USERS;", ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(
                        containsString("\"neo4j\", [\"admin\", \"PUBLIC\"], FALSE, FALSE, NULL"),
                        endsWithInteractiveExit);
    }

    @Test
    void promptsOnPasswordChangeRequiredSinceVersion4() throws Exception {
        assumeTrue(serverVersion.major() >= 4);

        testWithUser("bob", "expired", true)
                .args("--format verbose")
                .userInputLines("bob", "expired", "newpass", "newpass", "return 42 as x;", ":exit")
                .run()
                .assertSuccess()
                .assertThatOutput(
                        startsWith(
                                format(
                                        "username: bob%npassword: %nPassword change required%nnew password: %nconfirm password: %n")),
                        returned42AndExited());
    }

    @Test
    void promptsOnPasswordChangeRequiredBeforeVersion4() throws Exception {
        assumeTrue(serverVersion.major() < 4);

        testWithUser("bob", "expired", true)
                .args("--format verbose")
                .userInputLines("bob", "expired", "match (n) return count(n);", ":exit")
                .run()
                .assertSuccess(false)
                .assertThatErrorOutput(containsString("CALL dbms.changePassword"))
                .assertThatOutput(endsWithInteractiveExit);
    }

    @Test
    void allowUserToUpdateExpiredPasswordInteractivelyWithoutBeingPrompted() throws Exception {
        assumeTrue(serverVersion.major() >= 4);

        testWithUser("bob", "expired", true)
                .args("-u bob -p expired -d system --format verbose")
                .addArgs("ALTER CURRENT USER SET PASSWORD FROM \"expired\" TO \"shinynew\";")
                .run()
                .assertSuccess()
                .assertThatOutput(containsString("0 rows"));

        assertUserCanConnectAndRunQuery("bob", "shinynew");
    }

    @Test
    void shouldFailIfNonInteractivelySettingPasswordOnNonSystemDb() throws Exception {
        assumeTrue(serverVersion.major() >= 4);

        testWithUser("kjell", "expired", true)
                .args("-u kjell -p expired -d neo4j --non-interactive")
                .addArgs("ALTER CURRENT USER SET PASSWORD FROM \"expired\" TO \"höglund\";")
                .run()
                .assertFailure()
                .assertThatErrorOutput(containsString("The credentials you provided were valid, but must be changed"));
    }

    @Test
    void shouldBePromptedIfRunningNonInteractiveCypherThatDoesntUpdatePassword() throws Exception {
        assumeTrue(serverVersion.major() >= 4);

        testWithUser("bruce", "expired", true)
                .args("-u bruce -p expired -d neo4j")
                .addArgs("match (n) return n;")
                .userInputLines("newpass", "newpass")
                .run()
                .assertSuccess();

        assertUserCanConnectAndRunQuery("bruce", "newpass");
    }

    @Test
    void shouldNotBePromptedIfRunningWithExplicitNonInteractiveCypherThatDoesntUpdatePassword() throws Exception {
        assumeTrue(serverVersion.major() >= 4);

        testWithUser("nick", "expired", true)
                .args("-u nick -p expired -d neo4j --non-interactive")
                .addArgs("match (n) return n;")
                .run()
                .assertFailure()
                .assertThatErrorOutput(containsString("The credentials you provided were valid, but must be changed"))
                .assertThatOutput(emptyString());
    }

    @Test
    void doesPromptOnNonInteractiveOuput() throws Exception {
        testWithUser("holy", "ghost", false)
                .addArgs("return 42 as x;")
                .outputInteractive(false)
                .userInputLines("holy", "ghost")
                .run()
                .assertSuccessAndConnected()
                .assertOutputLines("username: holy", "password: ", "x", "42");
    }

    @Test
    void shouldHandleEmptyLine() throws Exception {
        var expectedPrompt = format("neo4j@neo4j> %n" + "neo4j@neo4j> :exit");

        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines("", ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(containsString(expectedPrompt), endsWithInteractiveExit);
    }

    @Test
    void wrongPortWithBolt() throws Exception {
        testWithUser("leonard", "coen", false)
                .args("-u leonard -p coen -a bolt://localhost:1234")
                .run()
                .assertFailure(
                        "Unable to connect to localhost:1234, ensure the database is running and that there is a working network connection to it.");
    }

    @Test
    void wrongPortWithNeo4j() throws Exception {
        testWithUser("jackie", "leven", false)
                .args("-u jackie -p leven -a neo4j://localhost:1234")
                .run()
                .assertFailure("Connection refused");
    }

    @Test
    void shouldAskForCredentialsWhenConnectingWithAFile() throws Exception {
        testWithUser("jacob", "collier", false)
                .addArgs("--file", fileFromResource("single.cypher"))
                .userInputLines("jacob", "collier")
                .run()
                .assertSuccessAndConnected()
                .assertOutputLines("username: jacob", "password: ", "result", "42");
    }

    @Test
    void shouldSupportVerboseFormatWhenReadingFile() throws Exception {
        var expectedQueryResult =
                format("+--------+%n" + "| result |%n" + "+--------+%n" + "| 42     |%n" + "+--------+");

        testWithUser("philip", "glass", false)
                .args("-u philip -p glass --format verbose")
                .addArgs("--file", fileFromResource("single.cypher"))
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(containsString(expectedQueryResult));
    }

    @Test
    void shouldReadEmptyCypherStatementsFile() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--file", fileFromResource("empty.cypher"))
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(emptyString());
    }

    @Test
    void shouldReadMultipleCypherStatementsFromFile() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--file", fileFromResource("multiple.cypher"))
                .run()
                .assertSuccessAndConnected()
                .assertOutputLines("result", "42", "result", "1337", "result", "\"done\"");
    }

    @Test
    void shouldFailIfInputFileDoesntExist() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--file", "missing-file")
                .run()
                .assertFailure("missing-file (No such file or directory)");
    }

    @Test
    void shouldHandleInvalidCypherFromFile() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--file", fileFromResource("invalid.cypher"))
                .run()
                .assertFailure()
                .assertThatErrorOutput(containsString("Invalid input"))
                .assertOutputLines("result", "42");
    }

    @Test
    void shouldReadSingleCypherStatementsFromFileInteractively() throws Exception {
        var file = fileFromResource("single.cypher");
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(":source " + file, ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(
                        containsString("> :source " + file + format("%nresult%n42")), endsWithInteractiveExit);
    }

    @Test
    void shouldDisconnect() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(":disconnect ", ":exit")
                .run()
                .assertSuccessAndDisconnected()
                .assertThatOutput(containsString("> :disconnect " + format("%nDisconnected>")), endsWith(GOOD_BYE));
    }

    @Test
    void shouldNotBeAbleToRunQueryWhenDisconnected() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(":disconnect ", "RETURN 42 AS x;", ":exit")
                .run()
                .assertThatErrorOutput(containsString("Not connected to Neo4j"))
                .assertThatOutput(containsString("> :disconnect " + format("%nDisconnected>")), endsWith(GOOD_BYE));
    }

    @Test
    void shouldDisconnectAndHelp() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(":disconnect ", ":help", ":exit")
                .run()
                .assertSuccessAndDisconnected()
                .assertThatOutput(
                        containsString("> :disconnect " + format("%nDisconnected> :help")),
                        containsString(format("%nAvailable commands:")),
                        endsWith(GOOD_BYE));
    }

    @Test
    void shouldDisconnectAndHistory() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(":disconnect ", ":history", ":exit")
                .run()
                .assertSuccessAndDisconnected()
                .assertThatOutput(
                        containsString("> :disconnect " + format("%nDisconnected> :history")),
                        containsString("1  :disconnect"),
                        containsString("2  :history"),
                        endsWith(GOOD_BYE));
    }

    @Test
    void shouldDisconnectAndSource() throws Exception {
        var file = fileFromResource("exit.cypher");
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(":disconnect ", ":source " + file)
                .run()
                .assertSuccessAndDisconnected()
                .assertThatOutput(
                        containsString("> :disconnect " + format("%nDisconnected> :source %s", file)),
                        endsWith(format("Bye!%n")));
    }

    @Test
    void shouldDisconnectAndConnectWithUsernamePasswordAndDatabase() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(
                        ":disconnect ", format(":connect -u %s -p %s -d %s", USER, PASSWORD, SYSTEM_DB_NAME), ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(
                        containsString("> :disconnect "
                                + format("%nDisconnected> :connect -u %s -p %s -d %s", USER, PASSWORD, SYSTEM_DB_NAME)),
                        endsWith(format("%s@%s> %s", USER, SYSTEM_DB_NAME, GOOD_BYE)));
    }

    @Test
    void shouldDisconnectAndConnectWithUsernamePasswordAndDatabaseWithFullArguments() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(
                        ":disconnect ",
                        format(":connect --username %s --password %s --database %s", USER, PASSWORD, SYSTEM_DB_NAME),
                        ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(
                        containsString("> :disconnect "
                                + format(
                                        "%nDisconnected> :connect --username %s --password %s --database %s",
                                        USER, PASSWORD, SYSTEM_DB_NAME)),
                        endsWith(format("%s@%s> %s", USER, SYSTEM_DB_NAME, GOOD_BYE)));
    }

    @Test
    void shouldFailIfConnectingWithInvalidPassword() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(
                        ":disconnect ", format(":connect -u %s -p %s -d %s", USER, "wut!", SYSTEM_DB_NAME), ":exit")
                .run()
                .assertSuccessAndDisconnected(false)
                .assertThatErrorOutput(containsString("The client is unauthorized due to authentication failure."))
                .assertThatOutput(
                        containsString("> :disconnect "
                                + format("%nDisconnected> :connect -u %s -p %s -d %s", USER, "wut!", SYSTEM_DB_NAME)),
                        endsWith(GOOD_BYE));
    }

    @Test
    void shouldFailIfConnectingWithInvalidUser() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(
                        ":disconnect ",
                        format(":connect -u %s -p %s -d %s", "PaulWesterberg", PASSWORD, SYSTEM_DB_NAME),
                        ":exit")
                .run()
                .assertSuccessAndDisconnected(false)
                .assertThatErrorOutput(containsString("The client is unauthorized due to authentication failure."))
                .assertThatOutput(
                        containsString("> :disconnect "
                                + format(
                                        "%nDisconnected> :connect -u %s -p %s -d %s",
                                        "PaulWesterberg", PASSWORD, SYSTEM_DB_NAME)),
                        endsWith(GOOD_BYE));
    }

    @Test
    void shouldDisconnectAndConnectWithUsernameAndPassword() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(":disconnect ", format(":connect -u %s -p %s", USER, PASSWORD), ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(
                        containsString(
                                "> :disconnect " + format("%nDisconnected> :connect -u %s -p %s", USER, PASSWORD)),
                        endsWith(format("%s@%s> %s", USER, DEFAULT_DEFAULT_DB_NAME, GOOD_BYE)));
    }

    @Test
    void shouldPromptForUsernameAndPasswordIfOnlyDBProvided() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(":disconnect ", format(":connect -d %s", SYSTEM_DB_NAME), USER, PASSWORD, ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(
                        containsString("> :disconnect " + format("%nDisconnected> :connect -d %s", SYSTEM_DB_NAME)),
                        containsString(format("%nusername: %s", USER) + format("%npassword: ***")),
                        endsWith(format("%s@%s> %s", USER, SYSTEM_DB_NAME, GOOD_BYE)));
    }

    @Test
    void shouldPromptForPasswordIfOnlyUserProvided() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(":disconnect ", format(":connect -d %s", SYSTEM_DB_NAME), USER, PASSWORD, ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(
                        containsString("> :disconnect " + format("%nDisconnected> :connect -d %s", SYSTEM_DB_NAME)),
                        containsString(format("%nusername: %s", USER) + format("%npassword: ***")),
                        endsWith(format("%s@%s> %s", USER, SYSTEM_DB_NAME, GOOD_BYE)));
    }

    @Test
    void shouldPromptForUsernameAndPasswordIfNoArgumentsProvided() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(":disconnect ", ":connect", USER, PASSWORD, ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(
                        containsString("> :disconnect " + format("%nDisconnected> :connect")),
                        containsString(format("%nusername: %s", USER) + format("%npassword: ***")),
                        endsWith(GOOD_BYE));
    }

    @Test
    void shouldReadMultipleCypherStatementsFromFileInteractively() throws Exception {
        var file = fileFromResource("multiple.cypher");
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(":source " + file, ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(
                        containsString("> :source " + file + format("%nresult%n42%nresult%n1337%nresult%n\"done\"")),
                        endsWithInteractiveExit);
    }

    @Test
    void shouldReadEmptyCypherStatementsFromFileInteractively() throws Exception {
        var file = fileFromResource("empty.cypher");
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(":source " + file, ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(containsString("> :source " + file + newLine + USER + "@"), endsWithInteractiveExit);
    }

    @Test
    void shouldHandleInvalidCypherStatementsFromFileInteractively() throws Exception {
        var file = fileFromResource("invalid.cypher");
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(":source " + file, ":exit")
                .run()
                .assertSuccessAndConnected(false)
                .assertThatErrorOutput(containsString("Invalid input"))
                .assertThatOutput(
                        containsString("> :source " + file + format("%nresult%n42%n") + USER + "@"),
                        endsWithInteractiveExit);
    }

    @Test
    void shouldFailIfInputFileDoesntExistInteractively() throws Exception {
        var file = "this-is-not-a-file";
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(":source " + file, ":exit")
                .run()
                .assertSuccessAndConnected(false)
                .assertThatErrorOutput(is("Cannot find file: '" + file + "'" + newLine))
                .assertThatOutput(containsString("> :source " + file + newLine + USER + "@"), endsWithInteractiveExit);
    }

    @Test
    void doesNotStartWhenDefaultDatabaseUnavailableIfInteractive() {
        // Multiple databases are only available from 4.0
        assumeTrue(serverVersion.major() >= 4);

        withDefaultDatabaseStopped(() -> buildTest()
                .addArgs("-u", USER, "-p", PASSWORD)
                .run()
                .assertFailure()
                .assertThatErrorOutput(containsDatabaseIsUnavailable(DEFAULT_DEFAULT_DB_NAME))
                .assertOutputLines());
    }

    @Test
    void startsAgainstSystemDatabaseWhenDefaultDatabaseUnavailableIfInteractive() {
        // Multiple databases are only available from 4.0
        assumeTrue(serverVersion.major() >= 4);

        withDefaultDatabaseStopped(() -> buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "-d", SYSTEM_DB_NAME)
                .userInputLines(":exit")
                .run()
                .assertSuccessAndConnected());
    }

    @Test
    void switchingToUnavailableDatabaseIfInteractive() {
        // Multiple databases are only available from 4.0
        assumeTrue(serverVersion.major() >= 4);

        withDefaultDatabaseStopped(() -> buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "-d", SYSTEM_DB_NAME)
                .userInputLines(":use " + DEFAULT_DEFAULT_DB_NAME, ":exit")
                .run()
                .assertSuccessAndConnected(false)
                .assertThatErrorOutput(containsDatabaseIsUnavailable(DEFAULT_DEFAULT_DB_NAME))
                .assertThatOutput(endsWithInteractiveExit));
    }

    @Test
    void switchingToUnavailableDefaultDatabaseIfInteractive() {
        // Multiple databases are only available from 4.0
        assumeTrue(serverVersion.major() >= 4);

        withDefaultDatabaseStopped(() -> buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "-d", SYSTEM_DB_NAME)
                .userInputLines(":use", ":exit")
                .run()
                .assertSuccessAndConnected(false)
                .assertThatErrorOutput(containsDatabaseIsUnavailable(DEFAULT_DEFAULT_DB_NAME))
                .assertThatOutput(endsWithInteractiveExit));
    }

    @Test
    void shouldChangePassword() throws Exception {
        testWithUser("kate", "bush", false)
                .args("--change-password")
                .userInputLines("kate", "bush", "betterpassword", "betterpassword")
                .run()
                .assertSuccess()
                .assertOutputLines("username: kate", "password: ", "new password: ", "confirm password: ");

        assertUserCanConnectAndRunQuery("kate", "betterpassword");
    }

    @Test
    void shouldChangePasswordWhenRequired() throws Exception {
        testWithUser("paul", "simon", true)
                .args("--change-password")
                .userInputLines("paul", "simon", "newpassword", "newpassword")
                .run()
                .assertSuccess()
                .assertOutputLines("username: paul", "password: ", "new password: ", "confirm password: ");

        assertUserCanConnectAndRunQuery("paul", "newpassword");
    }

    @Test
    void shouldChangePasswordWithUser() throws Exception {
        testWithUser("mike", "oldfield", false)
                .args("-u mike --change-password")
                .userInputLines("oldfield", "newfield", "newfield")
                .run()
                .assertSuccess()
                .assertOutputLines("password: ", "new password: ", "confirm password: ");

        assertUserCanConnectAndRunQuery("mike", "newfield");
    }

    @Test
    void shouldFailToChangePassword() throws Exception {
        testWithUser("led", "zeppelin", false)
                .args("-u led --change-password")
                .userInputLines("FORGOT MY PASSWORD", "robert", "robert")
                .run()
                .assertFailure()
                .assertThatErrorOutput(startsWith("Failed to change password"))
                .assertOutputLines("password: ", "new password: ", "confirm password: ");
    }

    @Test
    void shouldHandleMultiLineHistory() throws Exception {
        var expected =
                """
                > :history
                 1  return
                    'hej' as greeting;
                 2  return
                    1
                    as
                    x
                    ;
                 3  :history
                """;

        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines("return", "'hej' as greeting;", "return", "1", "as", "x", ";", ":history", ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(containsString(expected), endsWithInteractiveExit);
    }

    @Test
    void clearHistory() throws ArgumentParserException, IOException {
        var history = Files.createTempFile("temp-history", null);

        // Build up some history
        buildTest()
                .historyFile(history.toFile())
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines("return 1;", "return 2;", ":exit")
                .run()
                .assertSuccessAndConnected();

        var readHistory = Files.readAllLines(history);
        assertEquals(3, readHistory.size());
        assertThat(readHistory.get(0), endsWith("return 1;"));
        assertThat(readHistory.get(1), endsWith("return 2;"));
        assertThat(readHistory.get(2), endsWith(":exit"));

        var expected1 =
                """
                > :history
                 1  return 1;
                 2  return 2;
                 3  :exit
                 4  return 3;
                 5  :history""";
        var expected2 = """
                > :history
                 1  :history

                """;

        // Build up more history and clear
        buildTest()
                .historyFile(history.toFile())
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines("return 3;", ":history", ":history clear", ":history", ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(containsString(expected1), containsString(expected2));

        var readHistoryAfterClear = Files.readAllLines(history);
        assertEquals(2, readHistoryAfterClear.size());
        assertThat(readHistoryAfterClear.get(0), endsWith(":history"));
        assertThat(readHistoryAfterClear.get(1), endsWith(":exit"));
    }

    @Test
    public void failGracefullyOnUnknownCommands() throws ArgumentParserException, IOException {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD)
                .userInputLines(":non-existing-command")
                .run()
                .assertThatErrorOutput(
                        is("Could not find command :non-existing-command, use :help to see available commands\n"));
    }

    @Test
    void shouldDisconnectAndReconnectAsOtherUser() throws Exception {
        assumeAtLeastVersion("4.2.0");

        testWithUser("new_user", "new_password", false)
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(
                        ":disconnect", ":connect -u new_user -p new_password -d neo4j", "show current user yield user;")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(containsString(
                        "show current user yield user;\n" + "user\n" + "\"new_user\"\n" + "new_user@neo4j>"));
    }

    @Test
    void shouldDisconnectAndFailToReconnect() throws Exception {
        testWithUser("new_user", "new_password", false)
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(
                        ":disconnect",
                        ":connect -u new_user -p " + PASSWORD + " -d neo4j", // Wrong password
                        "show current user yield user;")
                .run()
                .assertThatOutput(containsString("neo4j@neo4j> :disconnect\n" + "Disconnected> :connect -u new_user -p "
                        + PASSWORD + " -d neo4j\n" + "Disconnected> show current user yield user;\n"
                        + "Disconnected>"))
                .assertThatErrorOutput(
                        containsString("The client is unauthorized due to authentication failure"),
                        containsString("Not connected"));
    }

    @Test
    void shouldDisconnectAndFailToReconnectInteractively() throws Exception {
        testWithUser("new_user", "new_password", false)
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(
                        ":disconnect",
                        ":connect -u new_user -d neo4j",
                        PASSWORD, // Password prompt with WRONG password
                        "show current user yield user;")
                .run()
                .assertThatOutput(
                        containsString("neo4j@neo4j> :disconnect\n" + "Disconnected> :connect -u new_user -d neo4j\n"
                                + "password: ***\n"
                                + "Disconnected> show current user yield user;\n"
                                + "Disconnected>"))
                .assertThatErrorOutput(
                        containsString("The client is unauthorized due to authentication failure"),
                        containsString("Not connected"));
    }

    @Test
    void shouldNotConnectIfAlreadyConnected() throws Exception {
        assumeAtLeastVersion("4.2.0");

        testWithUser("new_user", "new_password", false)
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(
                        ":connect -u new_user -p new_password -d neo4j", // No disconnect
                        "show current user yield user;")
                .run()
                .assertThatErrorOutput(containsString("Already connected"))
                .assertThatOutput(containsString("neo4j@neo4j> :connect -u new_user -p new_password -d neo4j\n"
                        + "neo4j@neo4j> show current user yield user;\n"
                        + "user\n"
                        + "\"neo4j\"\n"
                        + "neo4j@neo4j> "));
    }

    @Test
    void shouldIndentLineContinuations() throws ArgumentParserException, IOException {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines("return", "1 as res", ";", ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(containsString("neo4j@neo4j> return\n" + "             1 as res\n"
                        + "             ;\n"
                        + "res\n"
                        + "1\n"
                        + "neo4j@neo4j> :exit"));
    }

    @Test
    void evaluatesParameterArguments() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .addArgs("--param", "purple => 'rain'")
                .addArgs("--param", "advice => ['talk', 'less', 'smile', 'more']")
                .addArgs("--param", "when => date('2021-01-12')")
                .addArgs("--param", "repeatAfterMe => 'A' + 'B' + 'C'")
                .addArgs("--param", "easyAs => 1 + 2 + 3")
                .userInputLines(":params", "return $purple, $advice, $when, $repeatAfterMe, $easyAs;")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(
                        containsString(
                                """
                                 > :params
                                 :param advice        => ['talk', 'less', 'smile', 'more']
                                 :param easyAs        => 1 + 2 + 3
                                 :param purple        => 'rain'
                                 :param repeatAfterMe => 'A' + 'B' + 'C'
                                 :param when          => date('2021-01-12')"""),
                        containsString(
                                """
                                 > return $purple, $advice, $when, $repeatAfterMe, $easyAs;
                                 $purple, $advice, $when, $repeatAfterMe, $easyAs
                                 "rain", ["talk", "less", "smile", "more"], 2021-01-12, "ABC", 6
                                 """));
    }

    @Test
    void evaluatesArgumentsInteractive() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(
                        ":param purple => 'rain'",
                        ":param advice => ['talk', 'less', 'smile', 'more']",
                        ":param when => date('2021-01-12')",
                        ":param repeatAfterMe => 'A' + 'B' + 'C'",
                        ":param easyAs => 1 + 2 + 3",
                        ":params",
                        "return $purple, $advice, $when, $repeatAfterMe, $easyAs;")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(
                        containsString(
                                """
                                        > :params
                                        :param advice        => ['talk', 'less', 'smile', 'more']
                                        :param easyAs        => 1 + 2 + 3
                                        :param purple        => 'rain'
                                        :param repeatAfterMe => 'A' + 'B' + 'C'
                                        :param when          => date('2021-01-12')"""),
                        containsString(
                                """
                                        > return $purple, $advice, $when, $repeatAfterMe, $easyAs;
                                        $purple, $advice, $when, $repeatAfterMe, $easyAs
                                        "rain", ["talk", "less", "smile", "more"], 2021-01-12, "ABC", 6
                                        """));
    }

    @Test
    void shouldShowPlanDescription() throws Exception {
        assumeAtLeastVersion("4.4.0");

        var expected = serverVersion.major() >= 5
                ? """
                                +-----------------+----+---------+----------------+---------------------+
                                | Operator        | Id | Details | Estimated Rows | Pipeline            |
                                +-----------------+----+---------+----------------+---------------------+
                                | +ProduceResults |  0 | n       |             10 |                     |
                                | |               +----+---------+----------------+                     |
                                | +AllNodesScan   |  1 | n       |             10 | Fused in Pipeline 0 |
                                +-----------------+----+---------+----------------+---------------------+
                                """
                : """
                               +-----------------------+---------+----------------+---------------------+
                               | Operator              | Details | Estimated Rows | Other               |
                               +-----------------------+---------+----------------+---------------------+
                               | +ProduceResults@neo4j | n       |             10 | Fused in Pipeline 0 |
                               | |                     +---------+----------------+---------------------+
                               | +AllNodesScan@neo4j   | n       |             10 | Fused in Pipeline 0 |
                               +-----------------------+---------+----------------+---------------------+
                               """;

        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "verbose")
                .userInputLines("EXPLAIN MATCH (n) RETURN n;", ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(containsString(expected));
    }

    @Test
    void shouldImpersonate() throws Exception {
        assumeAtLeastVersion("4.4.0");

        // Setup impersonated user and role
        runInSystemDb(shell -> {
            createOrReplaceUser(shell, "impersonate_me", "123", false);
            shell.execute(cypher("CREATE OR REPLACE ROLE restricted AS COPY OF reader;"));
            shell.execute(cypher("DENY READ {secretProp} ON GRAPHS * TO restricted;"));
            shell.execute(cypher("GRANT ROLE restricted TO impersonate_me;"));
        });

        // Setup data
        runInDb(DEFAULT_DEFAULT_DB_NAME, shell -> {
            shell.execute(cypher("MERGE (n:ImpersonationTest { secretProp: 'hello', otherProp: 'hi' });"));
        });

        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--impersonate", "impersonate_me", "--format", "verbose")
                .userInputLines(
                        ":impersonate",
                        ":impersonate impersonate_me",
                        ":impersonate " + USER,
                        ":impersonate impersonate_me",
                        "MATCH (n:ImpersonationTest) RETURN n;",
                        ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(
                        containsString("as user neo4j impersonating impersonate_me"),
                        containsString(
                                """
                                neo4j(impersonate_me)@neo4j> :impersonate
                                neo4j@neo4j> :impersonate impersonate_me
                                neo4j(impersonate_me)@neo4j> :impersonate neo4j
                                neo4j@neo4j> :impersonate impersonate_me
                                neo4j(impersonate_me)@neo4j> MATCH (n:ImpersonationTest) RETURN n;
                                +----------------------------------------+
                                | n                                      |
                                +----------------------------------------+
                                | (:ImpersonationTest {otherProp: "hi"}) |
                                +----------------------------------------+
                                """),
                        endsWithInteractiveExit);
    }

    @Test
    void shouldDenyImpersonate() throws Exception {
        assumeAtLeastVersion("4.4.0");

        // Setup impersonated user and role
        runInSystemDb(shell -> {
            createOrReplaceUser(shell, "impersonate_me", "123", false);
            createOrReplaceUser(shell, "alice", "abc", false);
            shell.execute(cypher("CREATE OR REPLACE ROLE restricted AS COPY OF reader;"));
            shell.execute(cypher("DENY READ {secretProp} ON GRAPHS * TO restricted;"));
            shell.execute(cypher("GRANT ROLE restricted TO impersonate_me;"));
            shell.execute(cypher("DENY IMPERSONATE (alice) ON DBMS TO restricted;"));
        });

        // Setup data
        runInDb(DEFAULT_DEFAULT_DB_NAME, shell -> {
            shell.execute(cypher("MERGE (n:ImpersonationTest { secretProp: 'hello', otherProp: 'hi' });"));
        });

        buildTest()
                .args("-u alice -p abc --format verbose")
                .userInputLines(":impersonate impersonate_me", "MATCH (n:ImpersonationTest) RETURN n;", ":exit")
                .run()
                .assertSuccess(false)
                .assertThatErrorOutput(containsString("Cannot impersonate user 'impersonate_me'"))
                .assertThatOutput(
                        containsString(
                                """
                                alice@neo4j> :impersonate impersonate_me
                                Disconnected> MATCH (n:ImpersonationTest) RETURN n;
                                Disconnected> :exit
                                """),
                        endsWithInteractiveExit);
    }

    @Test
    void connectWithCredentialsInAddress() throws Exception {
        assumeAtLeastVersion("4.3");
        testWithUser("the_undertaker", "wwf", false)
                .args("-a neo4j://the_undertaker:wwf@localhost:7687 --format plain")
                .userInputLines("SHOW CURRENT USER YIELD user;", ":exit")
                .run()
                .assertSuccess()
                .assertThatOutput(
                        containsString(
                                """
                                user
                                "the_undertaker"
                                """),
                        endsWithInteractiveExit);
    }

    @Test
    void connectWithEnvironmentalAddress() throws Exception {
        assumeAtLeastVersion("4.3");
        testWithUser("hulk_hogan", "wwf2", false)
                .args("--format plain")
                .addEnvVariable("NEO4J_ADDRESS", "neo4j://hulk_hogan:wwf2@localhost:7687")
                .userInputLines("SHOW CURRENT USER YIELD user;", ":exit")
                .run()
                .assertSuccess()
                .assertThatOutput(
                        containsString(
                                """
                                user
                                "hulk_hogan"
                                """),
                        endsWithInteractiveExit);
    }

    @Test
    void disconnectOnClose() throws ArgumentParserException, IOException {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--file", fileFromResource("empty.cypher"))
                .run(true)
                .assertSuccessAndDisconnected()
                .assertThatOutput(emptyString());
    }

    @Test
    void logToFile() throws Exception {
        final var tempFile = Files.createTempFile("temp-log", null);
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--log", tempFile.toString())
                .userInputLines("return 1;", ":exit")
                .run()
                .assertSuccess();

        assertFileContains(tempFile, "Executing cypher: return 1");
        assertFileContains(tempFile, "org.neo4j.driver.internal.logging");

        Files.delete(tempFile);
    }

    private static CypherStatement cypher(String cypher) {
        return CypherStatement.complete(cypher);
    }

    private void assertUserCanConnectAndRunQuery(String user, String password) throws Exception {
        buildTest()
                .addArgs("-u", user, "-p", password, "--format", "plain", "return 42 as x;")
                .run()
                .assertSuccess();
    }

    private AssertableMain.AssertableMainBuilder buildTest() {
        return new TestBuilder().outputInteractive(true);
    }

    private AssertableMain.AssertableMainBuilder testWithUser(
            String name, String password, boolean requirePasswordChange) {
        runInSystemDb(shell -> createOrReplaceUser(shell, name, password, requirePasswordChange));
        return buildTest();
    }

    private void runInSystemDb(ThrowingConsumer<CypherShell, Exception> systemDbConsumer) {
        runInSystemDbAndReturn(shell -> {
            systemDbConsumer.accept(shell);
            return null;
        });
    }

    private void runInDb(String database, ThrowingConsumer<CypherShell, Exception> consumer) {
        runInDbAndReturn(database, shell -> {
            consumer.accept(shell);
            return null;
        });
    }

    private <T> T runInDbAndReturn(String database, ThrowingFunction<CypherShell, T, Exception> systemDbConsumer) {
        CypherShell shell = null;
        try {
            var boltHandler = new BoltStateHandler(false);
            var printer = new PrettyPrinter(new PrettyConfig(Format.PLAIN, false, 100));
            var parameters = ParameterService.create(boltHandler);
            shell = new CypherShell(new StringLinePrinter(), boltHandler, printer, parameters);
            shell.connect(testConnectionConfig("neo4j://localhost:7687")
                    .withUsernameAndPasswordAndDatabase(USER, PASSWORD, database));
            return systemDbConsumer.apply(shell);
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute statements during test setup: " + e.getMessage(), e);
        } finally {
            if (shell != null) {
                shell.disconnect();
            }
        }
    }

    private <T> T runInSystemDbAndReturn(ThrowingFunction<CypherShell, T, Exception> systemDbConsumer) {
        var systemDb = serverVersion.major() >= 4 ? "system" : ""; // Before version 4 we don't support multi databases
        return runInDbAndReturn(systemDb, systemDbConsumer);
    }

    private static void createOrReplaceUser(
            CypherShell shell, String name, String password, boolean requirePasswordChange) throws CommandException {
        if (majorVersion(shell.getServerVersion()) >= 4) {
            var changeString = requirePasswordChange ? "" : " CHANGE NOT REQUIRED";
            shell.execute(CypherStatement.complete(
                    "CREATE OR REPLACE USER " + name + " SET PASSWORD '" + password + "'" + changeString + ";"));
            shell.execute(CypherStatement.complete("GRANT ROLE reader TO " + name + ";"));
        } else {
            try {
                shell.execute(CypherStatement.complete("CALL dbms.security.createUser('" + name + "', '" + password
                        + "', " + requirePasswordChange + ")"));
            } catch (ClientException e) {
                if (e.code().equalsIgnoreCase("Neo.ClientError.General.InvalidArguments")
                        && e.getMessage().contains("already exists")) {
                    shell.execute(CypherStatement.complete("CALL dbms.security.deleteUser('" + name + "')"));
                    var createUser = "CALL dbms.security.createUser('" + name + "', '" + password + "', "
                            + requirePasswordChange + ")";
                    shell.execute(CypherStatement.complete(createUser));
                }
            }
        }
    }

    private String return42Output() {
        return format("> return 42 as x;%n" + return42VerboseTable());
    }

    private String return42VerboseTable() {
        return format("+----+%n" + "| x  |%n" + "+----+%n" + "| 42 |%n" + "+----+%n" + "%n" + "1 row");
    }

    private String fileFromResource(String filename) {
        return requireNonNull(getClass().getClassLoader().getResource(filename)).getFile();
    }

    private void withDefaultDatabaseStopped(ThrowingAction<Exception> test) {
        try {
            runInSystemDb(shell -> shell.execute(
                    CypherStatement.complete("STOP DATABASE " + DatabaseManager.DEFAULT_DEFAULT_DB_NAME + ";")));
            test.apply();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            runInSystemDb(shell -> shell.execute(
                    CypherStatement.complete("START DATABASE " + DatabaseManager.DEFAULT_DEFAULT_DB_NAME + ";")));
        }
    }

    private void assumeAtLeastVersion(String version) {
        assumeTrue(serverVersion.compareTo(Versions.version(version)) > 0);
    }

    private static class TestBuilder extends AssertableMain.AssertableMainBuilder {

        @Override
        public AssertableMain run(boolean closeMain) throws ArgumentParserException, IOException {
            assertNull(runnerFactory);
            assertNull(shell);
            var args = parseArgs();
            var outPrintStream = new PrintStream(out);
            var errPrintStream = new PrintStream(err);
            var logger = new AnsiPrinter(Format.VERBOSE, outPrintStream, errPrintStream);
            var terminal = terminalBuilder()
                    .dumb()
                    .streams(in, outPrintStream)
                    .simplePromptSupplier(() -> new TestSimplePrompt(in, new PrintWriter(out)))
                    .interactive(!args.getNonInteractive())
                    .logger(logger)
                    .build();
            Logger.setupLogging(args);
            var main = new Main(args, outPrintStream, errPrintStream, isOutputInteractive, terminal);
            var exitCode = main.startShell();

            if (closeMain) {
                main.close();
            }

            return new AssertableMain(exitCode, out, err, main.getCypherShell());
        }
    }

    private static void assertFileContains(Path file, String find) throws IOException {
        try (var reader = new BufferedReader(new FileReader(file.toFile()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.contains(find)) {
                    return;
                }
            }
        }
        fail("Could not find '" + find + "' in file " + file);
    }

    private Matcher<String> containsDatabaseIsUnavailable(String name) {
        return anyOf(
                containsString("database is unavailable"), containsString("Database '" + name + "' is unavailable"));
    }
}
