package io.bifroest.bifroest.systems.rebuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.kohsuke.MetaInfServices;

import io.bifroest.commons.boot.interfaces.Subsystem;
import io.bifroest.commons.statistics.eventbus.EventBusManager;
import io.bifroest.commons.statistics.units.SI_PREFIX;
import io.bifroest.commons.statistics.units.TIME_UNIT;
import io.bifroest.commons.statistics.units.parse.TimeUnitParser;
import io.bifroest.commons.statistics.units.parse.UnitParser;
import io.bifroest.commons.util.json.JSONUtils;
import io.bifroest.bifroest.systems.BifroestIdentifiers;
import io.bifroest.bifroest.systems.cassandra.EnvironmentWithCassandra;
import io.bifroest.bifroest.systems.prefixtree.EnvironmentWithMutablePrefixTree;
import io.bifroest.bifroest.systems.prefixtree.PrefixTree;
import io.bifroest.bifroest.systems.rebuilder.statistics.RebuildFinishedEvent;
import io.bifroest.bifroest.systems.rebuilder.statistics.RebuildStartedEvent;
import io.bifroest.commons.SystemIdentifiers;
import io.bifroest.commons.configuration.EnvironmentWithJSONConfiguration;
import io.bifroest.retentions.bootloader.EnvironmentWithRetentionStrategy;

@MetaInfServices
public class TreeRebuilderSystem< E extends EnvironmentWithJSONConfiguration & EnvironmentWithCassandra & EnvironmentWithMutablePrefixTree & EnvironmentWithMutableTreeRebuilder & EnvironmentWithRetentionStrategy >
        implements Subsystem<E> {

    private static final Logger log = LogManager.getLogger();

    private static final UnitParser parser = new TimeUnitParser( SI_PREFIX.ONE, TIME_UNIT.SECOND );
    private static final String DEFAULT_TREE_STORAGE = "/tmp/graphite/bifroest/tree";

    private Writer toStorageFile;

    @Override
    public String getSystemIdentifier() {
        return BifroestIdentifiers.REBUILDER;
    }

    @Override
    public Collection<String> getRequiredSystems() {
        return Arrays.asList( SystemIdentifiers.STATISTICS, BifroestIdentifiers.CASSANDRA, SystemIdentifiers.RETENTION );
    }

    @Override
    public void boot( final E environment ) throws Exception {
        JSONObject config = JSONUtils.getWithDefault( environment.getConfiguration(), "bifroest", new JSONObject() );
        String storage = JSONUtils.getWithDefault( config, "treestorage", DEFAULT_TREE_STORAGE );
        
        long nameRetention = parser.parse(config.getString("nameRetention")).longValue();

        try (BufferedReader fromStorageFile = new BufferedReader( new InputStreamReader( new GZIPInputStream( Files.newInputStream( Paths.get( storage ) ) ), Charset.forName("UTF-8") ) )) {
            PrefixTree tree = new PrefixTree( nameRetention );
            String line;
            int malformedLines = 0;
            int goodLines = 0;
            while( ( line = fromStorageFile.readLine() ) != null ) {
                String[] lineparts = StringUtils.split( line, ',' );
                if ( lineparts.length >= 2 ) {
                    goodLines++;
                    tree.addEntry( lineparts[0], Long.parseLong(lineparts[1]) );
                } else {
                    malformedLines++;
                    log.warn( "Ignored Line: " + line );
                }
            }
            log.info( "Parsed {} lines, Ignored {} lines in tree file", goodLines, malformedLines );
            environment.setTree( tree );
        } catch ( IOException e ) {
            log.warn( "Exception while reading prefix tree from disk", e );
            environment.setTree( new PrefixTree( nameRetention ) );
        }

        // Open this HERE, so this crashes early if we cannot write to this file.
        // Also, it is already open in case of an OOM, so we have a chance to successfully write the tree
        toStorageFile = new OutputStreamWriter( new GZIPOutputStream( Files.newOutputStream( Paths.get( storage ) ) ), Charset.forName("UTF-8") );

        environment.setRebuilder( new Rebuilder( environment) );
    }

    @Override
    public void shutdown( E environment ) {
        try {
            environment.getTree().forAllLeaves( ( metric, age ) -> {
                try {
                    toStorageFile.write( metric + "," + age + "\n" );
                } catch (Exception e) {
                    log.warn( "Exception while writing prefix tree to disk", e );
                }
            } );

            toStorageFile.close();
        } catch ( IOException e ) {
            log.warn( "Exception while writing prefix tree to disk", e );
        }
    }

    private class Rebuilder implements TreeRebuilder {

        private final E environment;

        public Rebuilder( E environment ) {
            this.environment = environment;
        }

        @Override
        public void rebuild() {
            EventBusManager.fire( new RebuildStartedEvent( Clock.systemUTC() ) );
            environment.getTree().removeOldObjects();
            EventBusManager.fire( new RebuildFinishedEvent( Clock.systemUTC() ) );
        }
    }

    @Override
    public void configure( JSONObject configuration ) {
        // empty
    }
}
