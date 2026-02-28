package io.fixreplay.cli;

import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IVersionProvider;

@Command(
    name = "fix-replay",
    mixinStandardHelpOptions = true,
    versionProvider = FixReplayCli.VersionProvider.class,
    description = "Run FIX replay checks from command line"
)
public final class FixReplayCli implements Callable<Integer> {
    @Override
    public Integer call() {
        System.out.println("fix-replay-core version " + currentVersion());
        return 0;
    }

    static String currentVersion() {
        String version = System.getProperty("fix.replay.version");
        return version == null || version.isBlank() ? "0.1.0-SNAPSHOT" : version;
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new FixReplayCli()).execute(args);
        System.exit(exitCode);
    }

    public static final class VersionProvider implements IVersionProvider {
        @Override
        public String[] getVersion() {
            return new String[] {"fix-replay-core " + currentVersion()};
        }
    }
}
