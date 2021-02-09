/*
 * Copyright (c) 2016 Santanu Sinha <santanu.sinha@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.appform.dropwizard.discovery.bundle.id;

import com.google.common.collect.ImmutableList;
import io.appform.dropwizard.discovery.bundle.id.constraints.IdValidationConstraint;
import io.appform.dropwizard.discovery.bundle.id.constraints.impl.JavaHashCodeBasedKeyPartitioner;
import io.appform.dropwizard.discovery.bundle.id.constraints.impl.PartitionValidator;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Test;

import java.time.*;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Test for {@link IdGenerator}
 */
@Slf4j
public class IdGeneratorTest {

    @Getter
    private static final class Runner implements Callable<Long> {
        private boolean stop = false;
        private long count = 0L;

        @Override
        public Long call() throws Exception {
            while (!stop) {
                Id id = IdGenerator.generate("X");
                count++;
            };
            return count;
        }
    }

    @Getter
    private static final class ConstraintRunner implements Callable<Long> {
        private final IdValidationConstraint constraint;
        private boolean stop = false;
        private long count = 0L;

        private ConstraintRunner(IdValidationConstraint constraint) {
            this.constraint = constraint;
        }

        @Override
        public Long call() throws Exception {
            while (!stop) {
                Optional<Id> id = IdGenerator.generateWithConstraints("X", Collections.singletonList(constraint));
                Assert.assertTrue(id.isPresent());
                count++;
            };
            return count;
        }
    }

    @Test
    public void testGenerate() throws Exception {
        IdGenerator.initialize(23);
        int numRunners = 20;

        ImmutableList.Builder<Runner> listBuilder = ImmutableList.builder();
        for (int i = 0; i < numRunners; i++) {
            listBuilder.add(new Runner());
        }

        List<Runner> runners = listBuilder.build();
        ExecutorService executorService = Executors.newFixedThreadPool(numRunners);
        for(Runner runner : runners) {
            executorService.submit(runner);
        }
        Awaitility.await()
                .pollInterval(Duration.ofSeconds(10))
                .timeout(Duration.ofSeconds(11))
                .until(() -> true);
        executorService.shutdownNow();

        long totalCount = runners.stream().mapToLong(Runner::getCount).sum();

        log.debug("Generated ID count: {}", totalCount);
        log.debug("Generated ID rate: {}/sec", totalCount/10);
        Assert.assertTrue(totalCount > 0);

    }


    @Test
    public void testGenerateWithConstraintsNoConstraint() throws Exception {
        IdGenerator.initialize(23);
        int numRunners = 20;

        ImmutableList.Builder<ConstraintRunner> listBuilder = ImmutableList.builder();
        for (int i = 0; i < numRunners; i++) {
            listBuilder.add(new ConstraintRunner(new PartitionValidator(4, new JavaHashCodeBasedKeyPartitioner(16))));
        }

        List<ConstraintRunner> runners = listBuilder.build();
        ExecutorService executorService = Executors.newFixedThreadPool(numRunners);
        for(ConstraintRunner runner : runners) {
            executorService.submit(runner);
        }
        Awaitility.await()
                .pollInterval(Duration.ofSeconds(10))
                .timeout(Duration.ofSeconds(11))
                .until(() -> true);
        executorService.shutdownNow();

        long totalCount = runners.stream().mapToLong(ConstraintRunner::getCount).sum();

        log.debug("Generated ID count: {}", totalCount);
        log.debug("Generated ID rate: {}/sec", totalCount/10);
        Assert.assertTrue(totalCount > 0);

    }

    @Test
    public void testConstraintFailure() {
        IdGenerator.initialize(23);
        Assert.assertFalse(IdGenerator.generateWithConstraints(
                "TST",
                ImmutableList.of(id -> false),
                false).isPresent());
    }

    @Test
    public void testParseFailure() {
        //Null or Empty String
        Assert.assertFalse(IdGenerator.parse(null).isPresent());
        Assert.assertFalse(IdGenerator.parse("").isPresent());

        //Invalid length
        Assert.assertFalse(IdGenerator.parse("TEST").isPresent());

        //Invalid chars
        Assert.assertFalse(IdGenerator.parse("XCL983dfb1ee0a847cd9e7321fcabc2f223").isPresent());
        Assert.assertFalse(IdGenerator.parse("XCL98-3df-b1e:e0a847cd9e7321fcabc2f223").isPresent());

        //Invalid month
        Assert.assertFalse(IdGenerator.parse("ABC2032250959030643972247").isPresent());
        //Invalid date
        Assert.assertFalse(IdGenerator.parse("ABC2011450959030643972247").isPresent());
        //Invalid hour
        Assert.assertFalse(IdGenerator.parse("ABC2011259659030643972247").isPresent());
        //Invalid minute
        Assert.assertFalse(IdGenerator.parse("ABC2011250972030643972247").isPresent());
        //Invalid sec
        Assert.assertFalse(IdGenerator.parse("ABC2011250959720643972247").isPresent());
    }

    @Test
    public void testParseSuccess(){
        String idString = "ABC2011250959030643972247";
        Optional<Id> idOptional = IdGenerator.parse(idString);
        Assert.assertTrue(idOptional.isPresent());

        Id id = idOptional.get();
        Assert.assertEquals(idString, id.getId());
        Assert.assertEquals(247, id.getExponent());
        Assert.assertEquals(3972, id.getNode());
        Assert.assertEquals(generateDate(2020, 11, 25, 9, 59, 3, 64, ZoneId.systemDefault()),
                id.getGeneratedDate());
    }

    @Test
    public void testParseSuccessAfterGeneration(){
        Id generatedId = IdGenerator.generate("TEST123");
        Optional<Id> parsedIdOptional = IdGenerator.parse(generatedId.getId());
        Assert.assertTrue(parsedIdOptional.isPresent());

        Id parsedId = parsedIdOptional.get();
        Assert.assertEquals(parsedId.getId(), generatedId.getId());
        Assert.assertEquals(parsedId.getExponent(), generatedId.getExponent());
        Assert.assertEquals(parsedId.getNode(), generatedId.getNode());
        Assert.assertEquals(parsedId.getGeneratedDate(), generatedId.getGeneratedDate());
    }


    private Date generateDate(int year, int month, int day, int hour, int min, int sec, int ms, ZoneId zoneId) {
        return Date.from(
                Instant.from(
                        ZonedDateTime.of(
                                LocalDateTime.of(
                                        year, month, day, hour, min, sec, Math.multiplyExact(ms, 1000000)
                                ),
                                zoneId
                        )
                )
        );
    }


}