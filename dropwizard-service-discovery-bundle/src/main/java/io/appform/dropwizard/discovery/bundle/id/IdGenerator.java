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

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.appform.dropwizard.discovery.bundle.id.constraints.IdValidationConstraint;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Id generation
 */
@Slf4j
public class IdGenerator {

    private static final int MINIMUM_ID_LENGTH = 22;

    private enum IdValidationState {
        VALID,
        INVALID_RETRYABLE,
        INVALID_NON_RETRYABLE
    }

    private static final class IdInfo {
        int exponent;
        long time;

        public IdInfo(int exponent, long time) {
            this.exponent = exponent;
            this.time = time;
        }
    }

    private static SecureRandom random = new SecureRandom(Long.toBinaryString(System.currentTimeMillis()).getBytes());
    private static int nodeId;
    private static DateTimeFormatter formatter = DateTimeFormat.forPattern("yyMMddHHmmssSSS");
    private static final CollisionChecker collisionChecker = new CollisionChecker();
    private static List<IdValidationConstraint> globalConstraints = Collections.emptyList();
    private static Map<String, List<IdValidationConstraint>> domainSpecificConstraints = new HashMap<>();
    private static final Retryer<GenerationResult> retrier = RetryerBuilder.<GenerationResult>newBuilder()
            .withStopStrategy(StopStrategies.stopAfterAttempt(512))
            .retryIfException()
            .retryIfResult(Objects::isNull)
            .retryIfResult(result -> result.getState().equals(IdValidationState.INVALID_RETRYABLE))
            .build();
    private static final String patternString = "(.*)([0-9]{15})([0-9]{4})([0-9]{3})";
    private static Pattern pattern = Pattern.compile(patternString);

    public static void initialize(int node) {
        nodeId = node;
    }

    public static synchronized void cleanUp() {
        globalConstraints.clear();
        domainSpecificConstraints.clear();
    }

    public static void initialize(
            int node, List<IdValidationConstraint> globalConstraints, Map<String, List<IdValidationConstraint>> domainSpecificConstraints) {
        nodeId = node;
        IdGenerator.globalConstraints = globalConstraints != null
                ? globalConstraints
                : Collections.emptyList();
        IdGenerator.domainSpecificConstraints.putAll(domainSpecificConstraints);
    }

    public static synchronized void registerGlobalConstraints(IdValidationConstraint... constraints) {
        registerGlobalConstraints(ImmutableList.copyOf(constraints));
    }

    public static synchronized void registerGlobalConstraints(List<IdValidationConstraint> constraints) {
        Preconditions.checkArgument(null != constraints && !constraints.isEmpty());
        if(null == globalConstraints) {
            globalConstraints = new ArrayList<>();
        }
        globalConstraints.addAll(constraints);
    }

    public static synchronized void registerDomainSpecificConstraints(String domain, IdValidationConstraint... validationConstraints) {
        registerDomainSpecificConstraints(domain, ImmutableList.copyOf(validationConstraints));
    }

    public static synchronized void registerDomainSpecificConstraints(String domain, List<IdValidationConstraint> validationConstraints) {
        Preconditions.checkArgument(null != validationConstraints && !validationConstraints.isEmpty());
        if(!domainSpecificConstraints.containsKey(domain)) {
            domainSpecificConstraints.put(domain, new ArrayList<>());
        }
        domainSpecificConstraints.get(domain).addAll(validationConstraints);
    }

    /**
     * Generate id with given prefix
     *
     * @param prefix String prefix with will be used to blindly merge
     * @return Generated Id
     */
    public static Id generate(String prefix) {
        final IdInfo idInfo = random();
        DateTime dateTime = new DateTime(idInfo.time);
        final String id = String.format("%s%s%04d%03d", prefix, formatter.print(dateTime), nodeId, idInfo.exponent);
        return Id.builder()
                .id(id)
                .exponent(idInfo.exponent)
                .generatedDate(dateTime.toDate())
                .node(nodeId)
                .build();
    }

    /**
     * Generate id that mathces all passed constraints.
     * NOTE: There are performance implications for this.
     * The evaluation of constraints will take it's toll on id generation rates. Tun rests to check speed.
     *
     * @param prefix        String prefix
     * @param domain Domain for constraint selection
     * @return
     */
    public static Optional<Id> generateWithConstraints(String prefix, String domain) {
        return generateWithConstraints(prefix, domainSpecificConstraints.getOrDefault(domain, Collections.emptyList()), true);
    }

    /**
     * Generate id that mathces all passed constraints.
     * NOTE: There are performance implications for this.
     * The evaluation of constraints will take it's toll on id generation rates. Tun rests to check speed.
     *
     * @param prefix        String prefix
     * @param domain Domain for constraint selection
     * @param skipGlobal Skip global constrains and use only passed ones
     * @return Id if it could be generated
     */
    public static Optional<Id> generateWithConstraints(String prefix, String domain, boolean skipGlobal) {
        return generateWithConstraints(prefix, domainSpecificConstraints.getOrDefault(domain, Collections.emptyList()), skipGlobal);
    }

    /**
     * Generate id that mathces all passed constraints.
     * NOTE: There are performance implications for this.
     * The evaluation of constraints will take it's toll on id generation rates. Tun rests to check speed.
     *
     * @param prefix        String prefix
     * @param inConstraints Constraints that need to be validate.
     * @return Id if it could be generated
     */
    public static Optional<Id> generateWithConstraints(String prefix, final List<IdValidationConstraint> inConstraints) {
        return generateWithConstraints(prefix, inConstraints, false);
    }

    /**
     * Generate id by parsing given string
     *
     * @param idString String idString
     * @return Id if it could be generated
     */
    public static Optional<Id> parse(final String idString) {
        if (idString == null
                || idString.length() < MINIMUM_ID_LENGTH) {
            return Optional.empty();
        }
        try {
            Matcher matcher = pattern.matcher(idString);
            if (matcher.find()) {
                return Optional.of(Id.builder()
                        .id(idString)
                        .node(Integer.parseInt(matcher.group(3)))
                        .exponent(Integer.parseInt(matcher.group(4)))
                        .generatedDate(formatter.parseDateTime(matcher.group(2)).toDate())
                        .build());
            }
            return Optional.empty();
        } catch (Exception e) {
            log.warn("Could not parse idString {}", e.getMessage());
            return Optional.empty();
        }
    }

    @Data
    private static class GenerationResult {
        private final Id id;
        private final IdValidationState state;
    }
    /**
     * Generate id that mathces all passed constraints.
     * NOTE: There are performance implications for this.
     * The evaluation of constraints will take it's toll on id generation rates. Tun rests to check speed.
     *
     * @param prefix        String prefix
     * @param inConstraints Constraints that need to be validate.
     * @param skipGlobal Skip global constrains and use only passed ones
     * @return Id if it could be generated
     */
    public static Optional<Id> generateWithConstraints(String prefix, final List<IdValidationConstraint> inConstraints, boolean skipGlobal) {
        try {
            final GenerationResult generationResult = retrier.call(() -> {
                Id id = generate(prefix);
                return new GenerationResult(id, validateId(inConstraints, id, skipGlobal));
            });
            return Optional.ofNullable(generationResult.getId());
        }
        catch (ExecutionException e) {
            log.error("Error occurred while generating id with prefix " + prefix, e);
        }
        catch (RetryException e) {
            log.error("Failed to generate id with prefix " + prefix + " after max attempts (512)", e);
        }
        return Optional.empty();
    }

    private static synchronized IdInfo random() {
        int randomGen;
        long time;
        do {
            time = System.currentTimeMillis();
            randomGen = random.nextInt(Constants.MAX_ID_PER_MS);
        } while (!collisionChecker.check(time, randomGen));
        return new IdInfo(randomGen, time);
    }

    private static IdValidationState validateId(List<IdValidationConstraint> inConstraints, Id id, boolean skipGlobal) {
        //First evaluate global constraints
        final IdValidationConstraint failedGlobalConstraint
                = skipGlobal || null == globalConstraints
                ? null
                : globalConstraints.stream()
                        .filter(constraint -> !constraint.isValid(id))
                        .findFirst()
                        .orElse(null);
        if(null != failedGlobalConstraint) {
            return failedGlobalConstraint.failFast()
                    ? IdValidationState.INVALID_NON_RETRYABLE
                    : IdValidationState.INVALID_RETRYABLE;
        }
        //Evaluate local + domain constraints
        final IdValidationConstraint failedLocalConstraint
                = null == inConstraints
                ? null
                : inConstraints.stream()
                .filter(constraint -> !constraint.isValid(id))
                .findFirst()
                .orElse(null);
        if(null != failedLocalConstraint) {
            return failedLocalConstraint.failFast()
                    ? IdValidationState.INVALID_NON_RETRYABLE
                    : IdValidationState.INVALID_RETRYABLE;
        }
        return IdValidationState.VALID;
    }
}
