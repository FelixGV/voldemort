/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.store;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

import voldemort.client.RoutingTier;
import voldemort.serialization.SerializerDefinition;
import voldemort.store.slop.strategy.HintedHandoffStrategyType;
import voldemort.store.system.SystemStoreConstants;
import voldemort.utils.Utils;

import com.google.common.base.Objects;

/**
 * The configuration information for a store.
 * 
 * 
 */
public class StoreDefinition implements Serializable {

    private static final long serialVersionUID = 1;

    private final String name;
    private final String type;
    private final String description;
    private final SerializerDefinition keySerializer;
    private final SerializerDefinition valueSerializer;
    private final SerializerDefinition transformsSerializer;
    private final RoutingTier routingPolicy;
    private final int replicationFactor;
    private final Integer preferredWrites;
    private final int requiredWrites;
    private final Integer preferredReads;
    private final int requiredReads;
    private final Integer retentionPeriodDays;
    private final Integer retentionScanThrottleRate;
    private final Integer retentionFrequencyDays;
    private final String routingStrategyType;
    private final String viewOf;
    private final HashMap<Integer, Integer> zoneReplicationFactor;
    private final Integer zoneCountReads;
    private final Integer zoneCountWrites;
    private final String valueTransformation;
    private final String serializerFactory;
    private final HintedHandoffStrategyType hintedHandoffStrategyType;
    private final Integer hintPrefListSize;
    private final List<String> owners;
    private final long memoryFootprintMB;

    public StoreDefinition(String name,
                           String type,
                           String description,
                           SerializerDefinition keySerializer,
                           SerializerDefinition valueSerializer,
                           SerializerDefinition transformsSerializer,
                           RoutingTier routingPolicy,
                           String routingStrategyType,
                           int replicationFactor,
                           Integer preferredReads,
                           int requiredReads,
                           Integer preferredWrites,
                           int requiredWrites,
                           String viewOfStore,
                           String valTrans,
                           HashMap<Integer, Integer> zoneReplicationFactor,
                           Integer zoneCountReads,
                           Integer zoneCountWrites,
                           Integer retentionDays,
                           Integer retentionThrottleRate,
                           Integer retentionFrequencyDays,
                           String factory,
                           HintedHandoffStrategyType hintedHandoffStrategyType,
                           Integer hintPrefListSize,
                           List<String> owners,
                           long memoryFootprintMB) {
        this.name = Utils.notNull(name);
        this.type = type;
        this.description = description;
        this.replicationFactor = replicationFactor;
        this.preferredReads = preferredReads;
        this.requiredReads = requiredReads;
        this.preferredWrites = preferredWrites;
        this.requiredWrites = requiredWrites;
        this.routingPolicy = routingPolicy;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.transformsSerializer = transformsSerializer;
        this.retentionPeriodDays = retentionDays;
        this.retentionScanThrottleRate = retentionThrottleRate;
        this.retentionFrequencyDays = retentionFrequencyDays;
        this.memoryFootprintMB = memoryFootprintMB;
        this.routingStrategyType = routingStrategyType;
        this.viewOf = viewOfStore;
        this.valueTransformation = valTrans;
        this.zoneReplicationFactor = zoneReplicationFactor;
        this.zoneCountReads = zoneCountReads;
        this.zoneCountWrites = zoneCountWrites;
        this.serializerFactory = factory;
        this.hintedHandoffStrategyType = hintedHandoffStrategyType;
        this.hintPrefListSize = hintPrefListSize;
        this.owners = owners;
    }

    private void throwIllegalException(String errorMessage) {
        throw new IllegalArgumentException(" Store '" + this.name + "'. Error: " + errorMessage);
    }

    protected void checkParameterLegality() {

        // null checks
        Utils.notNull(this.type);
        Utils.notNull(routingPolicy);
        Utils.notNull(keySerializer);
        Utils.notNull(valueSerializer);

        if(requiredReads < 1)
            throwIllegalException("Cannot have a requiredReads number less than 1.");
        else if(requiredReads > replicationFactor)
            throwIllegalException("Cannot have more requiredReads then there are replicas.");

        if(requiredWrites < 1)
            throwIllegalException("Cannot have a requiredWrites number less than 1.");
        else if(requiredWrites > replicationFactor)
            throwIllegalException("Cannot have more requiredWrites then there are replicas.");

        if(preferredWrites != null) {
            if(preferredWrites < requiredWrites)
                throwIllegalException("preferredWrites must be greater or equal to requiredWrites.");
            if(preferredWrites > replicationFactor)
                throwIllegalException("Cannot have more preferredWrites then there are replicas.");
        }
        if(preferredReads != null) {
            if(preferredReads < requiredReads)
                throwIllegalException("preferredReads must be greater or equal to requiredReads.");
            if(preferredReads > replicationFactor)
                throwIllegalException("Cannot have more preferredReads then there are replicas.");
        }

        if(retentionPeriodDays != null && retentionPeriodDays < 0)
            throwIllegalException("Retention days must be non-negative.");

        if(!SystemStoreConstants.isSystemStore(name) && zoneReplicationFactor != null
           && zoneReplicationFactor.size() != 0) {

            if(zoneCountReads == null || zoneCountReads < 0)
                throwIllegalException("Zone Counts reads must be non-negative / non-null");

            if(zoneCountWrites == null || zoneCountWrites < 0)
                throwIllegalException("Zone Counts writes must be non-negative");

            int sumZoneReplicationFactor = 0;
            int replicatingZones = 0;
            for(Integer zoneId: zoneReplicationFactor.keySet()) {
                int currentZoneRepFactor = zoneReplicationFactor.get(zoneId);

                sumZoneReplicationFactor += currentZoneRepFactor;
                if(currentZoneRepFactor > 0)
                    replicatingZones++;
            }

            if(replicatingZones <= 0) {
                throwIllegalException("Cannot have no zones to replicate to. "
                                      + "Should have some positive zoneReplicationFactor");
            }

            // Check if sum of individual zones is equal to total replication
            // factor
            if(sumZoneReplicationFactor != replicationFactor) {
                throwIllegalException("Sum total of zones (" + sumZoneReplicationFactor
                                      + ") does not match the total replication factor ("
                                      + replicationFactor + ")");
            }

            // Check if number of zone-count-reads and zone-count-writes are
            // less than zones replicating to
            if(zoneCountReads >= replicatingZones) {
                throwIllegalException("Number of zones to block for while reading ("
                                      + zoneCountReads
                                      + ") should be less then replicating zones ("
                                      + replicatingZones + ")");
            }

            if(zoneCountWrites >= replicatingZones) {
                throwIllegalException("Number of zones to block for while writing ("
                                      + zoneCountWrites
                                      + ") should be less then replicating zones ("
                                      + replicatingZones + ")");
            }
        }
    }

    public String getDescription() {
        return this.description;
    }

    public String getSerializerFactory() {
        return this.serializerFactory;
    }

    public boolean hasTransformsSerializer() {
        return transformsSerializer != null;
    }

    public String getName() {
        return name;
    }

    public int getRequiredWrites() {
        return requiredWrites;
    }

    public SerializerDefinition getKeySerializer() {
        return keySerializer;
    }

    public SerializerDefinition getValueSerializer() {
        return valueSerializer;
    }

    public SerializerDefinition getTransformsSerializer() {
        return transformsSerializer;
    }

    public RoutingTier getRoutingPolicy() {
        return this.routingPolicy;
    }

    public int getReplicationFactor() {
        return this.replicationFactor;
    }

    public String getRoutingStrategyType() {
        return routingStrategyType;
    }

    public int getRequiredReads() {
        return this.requiredReads;
    }

    public boolean hasPreferredWrites() {
        return preferredWrites != null;
    }

    public int getPreferredWrites() {
        return preferredWrites == null ? getRequiredWrites() : preferredWrites;
    }

    public int getPreferredReads() {
        return preferredReads == null ? getRequiredReads() : preferredReads;
    }

    public boolean hasPreferredReads() {
        return preferredReads != null;
    }

    public String getType() {
        return type;
    }

    public boolean hasRetentionPeriod() {
        return this.retentionPeriodDays != null && this.retentionPeriodDays > 0;
    }

    public Integer getRetentionDays() {
        return this.retentionPeriodDays;
    }

    public boolean hasRetentionScanThrottleRate() {
        return this.retentionScanThrottleRate != null;
    }

    public Integer getRetentionScanThrottleRate() {
        return this.retentionScanThrottleRate;
    }

    public boolean hasRetentionFrequencyDays() {
        return this.retentionFrequencyDays != null;
    }

    public Integer getRetentionFrequencyDays() {
        return this.retentionFrequencyDays;
    }

    public boolean isView() {
        return this.viewOf != null;
    }

    public String getViewTargetStoreName() {
        return viewOf;
    }

    public boolean hasValueTransformation() {
        return this.valueTransformation != null;
    }

    public String getValueTransformation() {
        return valueTransformation;
    }

    public HashMap<Integer, Integer> getZoneReplicationFactor() {
        return zoneReplicationFactor;
    }

    public Integer getZoneCountReads() {
        return zoneCountReads;
    }

    public boolean hasZoneCountReads() {
        return zoneCountReads != null;
    }

    public Integer getZoneCountWrites() {
        return zoneCountWrites;
    }

    public boolean hasZoneCountWrites() {
        return zoneCountWrites != null;
    }

    public HintedHandoffStrategyType getHintedHandoffStrategyType() {
        return hintedHandoffStrategyType;
    }

    public boolean hasHintedHandoffStrategyType() {
        return hintedHandoffStrategyType != null;
    }

    public Integer getHintPrefListSize() {
        return hintPrefListSize;
    }

    public boolean hasHintPreflistSize() {
        return hintPrefListSize != null;
    }

    public List<String> getOwners() {
        return this.owners;
    }

    public long getMemoryFootprintMB() {
        return this.memoryFootprintMB;
    }

    public boolean hasMemoryFootprint() {
        return memoryFootprintMB != 0;
    }

    @Override
    public boolean equals(Object o) {
        if(this == o)
            return true;
        else if(o == null)
            return false;
        else if(!(o.getClass() == StoreDefinition.class))
            return false;

        StoreDefinition def = (StoreDefinition) o;
        return getName().equals(def.getName())
               && getType().equals(def.getType())
               && getReplicationFactor() == def.getReplicationFactor()
               && getRequiredReads() == def.getRequiredReads()
               && Objects.equal(getPreferredReads(), def.getPreferredReads())
               && getRequiredWrites() == def.getRequiredWrites()
               && Objects.equal(getPreferredWrites(), def.getPreferredWrites())
               && getKeySerializer().equals(def.getKeySerializer())
               && getValueSerializer().equals(def.getValueSerializer())
               && Objects.equal(getTransformsSerializer() != null ? getTransformsSerializer() : null,
                                def.getTransformsSerializer() != null ? def.getTransformsSerializer() : null)
               && getRoutingPolicy() == def.getRoutingPolicy()
               && Objects.equal(getViewTargetStoreName(), def.getViewTargetStoreName())
               // FIXME: This comparison is irrelevant, but not changing it in case it breaks something...
               && Objects.equal(getValueTransformation() != null ? getValueTransformation().getClass() : null,
                                def.getValueTransformation() != null ? def.getValueTransformation().getClass() : null)
               // FIXME: This comparison is irrelevant, but not changing it in case it breaks something...
               && Objects.equal(getZoneReplicationFactor() != null ? getZoneReplicationFactor().getClass() : null,
                                def.getZoneReplicationFactor() != null ? def.getZoneReplicationFactor().getClass() : null)
               && Objects.equal(getZoneCountReads(), def.getZoneCountReads())
               && Objects.equal(getZoneCountWrites(), def.getZoneCountWrites())
               && Objects.equal(getRetentionDays(), def.getRetentionDays())
               && Objects.equal(getRetentionScanThrottleRate(), def.getRetentionScanThrottleRate())
               && Objects.equal(getSerializerFactory() != null ? getSerializerFactory() : null,
                                def.getSerializerFactory() != null ? def.getSerializerFactory() : null)
               && Objects.equal(getHintedHandoffStrategyType(), def.getHintedHandoffStrategyType())
               && Objects.equal(getHintPrefListSize(), def.getHintPrefListSize())
               && Objects.equal(getMemoryFootprintMB(), def.getMemoryFootprintMB());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getName(),
                                getType(),
                                getDescription(),
                                getKeySerializer(),
                                getValueSerializer(),
                                getTransformsSerializer(),
                                getRoutingPolicy(),
                                getRoutingStrategyType(),
                                getReplicationFactor(),
                                getRequiredReads(),
                                getRequiredWrites(),
                                getPreferredReads(),
                                getPreferredWrites(),
                                getViewTargetStoreName(),
                                // FIXME: This comparison is irrelevant, but not changing it in case it breaks something...
                                getValueTransformation() == null ? null : getValueTransformation().getClass(),
                                // FIXME: This comparison is irrelevant, but not changing it in case it breaks something...
                                getZoneReplicationFactor() == null ? null : getZoneReplicationFactor().getClass(),
                                getZoneCountReads(),
                                getZoneCountWrites(),
                                getRetentionDays(),
                                getRetentionScanThrottleRate(),
                                getSerializerFactory(),
                                hasHintedHandoffStrategyType() ? getHintedHandoffStrategyType() : null,
                                hasHintPreflistSize() ? getHintPrefListSize() : null,
                                getOwners(),
                                getMemoryFootprintMB());
    }

    @Override
    public String toString() {
        return "StoreDefinition(name = " + getName() + ", type = " + getType() + ", description = "
               + getDescription() + ", key-serializer = " + getKeySerializer()
               + ", value-serializer = " + getValueSerializer() + ", routing = "
               + getRoutingPolicy() + ", routing-strategy = " + getRoutingStrategyType()
               + ", replication = " + getReplicationFactor() + ", required-reads = "
               + getRequiredReads() + ", preferred-reads = " + getPreferredReads()
               + ", required-writes = " + getRequiredWrites() + ", preferred-writes = "
               + getPreferredWrites() + ", view-target = " + getViewTargetStoreName()
               + ", value-transformation = " + getValueTransformation() + ", retention-days = "
               + getRetentionDays() + ", throttle-rate = " + getRetentionScanThrottleRate()
               + ", zone-replication-factor = " + getZoneReplicationFactor()
               + ", zone-count-reads = " + getZoneCountReads() + ", zone-count-writes = "
               + getZoneCountWrites() + ", serializer factory = " + getSerializerFactory() + ")"
               + ", hinted-handoff-strategy = " + getHintedHandoffStrategyType()
               + ", hint-preflist-size = " + getHintPrefListSize() + ", owners = " + getOwners()
               + ", memory-footprint(MB)" + getMemoryFootprintMB() + ")";
    }

    public String diff(StoreDefinition other) {
        if (this.equals(other)) {
            return "StoreDefinitions are identical";
        } else {
            StringBuilder sb = new StringBuilder();
            if (!getName().equals(other.getName())) {
                addToDiff("Name", this.getName(), other.getName(), sb);
            }
            if (!getType().equals(other.getType())) {
                addToDiff("Type", this.getType(), other.getType(), sb);
            }
            if (getReplicationFactor() != other.getReplicationFactor()) {
                addToDiff("Replication factor", this.getReplicationFactor(), other.getReplicationFactor(), sb);
            }
            if (getRequiredReads() != other.getRequiredReads()) {
                addToDiff("Required reads", this.getRequiredReads(), other.getRequiredReads(), sb);
            }
            if (!Objects.equal(getPreferredReads(), other.getPreferredReads())) {
                addToDiff("Preferred reads", this.getPreferredReads(), other.getPreferredReads(), sb);
            }
            if (getRequiredWrites() != other.getRequiredWrites()) {
                addToDiff("Required writes", this.getRequiredWrites(), other.getRequiredWrites(), sb);
            }
            if (!Objects.equal(getPreferredWrites(), other.getPreferredWrites())) {
                addToDiff("Preferred writes", this.getPreferredWrites(), other.getPreferredWrites(), sb);
            }
            if (!getKeySerializer().equals(other.getKeySerializer())) {
                addToDiff("Key serializer", this.getKeySerializer().toString(), other.getKeySerializer().toString(), sb);
            }
            if (!getValueSerializer().equals(other.getValueSerializer())) {
                addToDiff("Value serializer", this.getValueSerializer().toString(), other.getValueSerializer().toString(), sb);
            }
            if (!Objects.equal(getTransformsSerializer() != null ? getTransformsSerializer(): null,
                    other.getTransformsSerializer() != null ? other.getTransformsSerializer(): null)) {
                addToDiff("Transforms Serializer", this.getTransformsSerializer().toString(), other.getTransformsSerializer().toString(), sb);
            }
            if (getRoutingPolicy() != other.getRoutingPolicy()) {
                addToDiff("Routing policy", this.getRoutingPolicy().toDisplay(), other.getRoutingPolicy().toDisplay(), sb);
            }
            if (!Objects.equal(getViewTargetStoreName(), other.getViewTargetStoreName())) {
                addToDiff("View target store name", this.getViewTargetStoreName(), other.getViewTargetStoreName(), sb);
            }
            if (!Objects.equal(getValueTransformation() != null ? getValueTransformation().getClass(): null,
                    other.getValueTransformation() != null ? other.getValueTransformation().getClass() : null)) {
                // FIXME: This comparison is irrelevant, but leaving it as is so that it yields the same result as equals()
                addToDiff("Value transformation", this.getValueTransformation(), other.getValueTransformation(), sb);
            }
            if (!Objects.equal(getZoneReplicationFactor() != null ? getZoneReplicationFactor().getClass(): null,
                    other.getZoneReplicationFactor() != null ? other.getZoneReplicationFactor().getClass(): null)) {
                // FIXME: This comparison is irrelevant, but leaving it as is so that it yields the same result as equals()
                addToDiff("Zone replication factor", this.getZoneReplicationFactor().toString(), other.getZoneReplicationFactor().toString(), sb);
            }
            if (!Objects.equal(getZoneCountReads(), other.getZoneCountReads())) {
                addToDiff("Zone count reads", this.getZoneCountReads(), other.getZoneCountReads(), sb);
            }
            if (!Objects.equal(getZoneCountWrites(), other.getZoneCountWrites())) {
                addToDiff("Zone count writes", this.getZoneCountWrites(), other.getZoneCountWrites(), sb);
            }
            if (!Objects.equal(getRetentionDays(), other.getRetentionDays())) {
                addToDiff("Retention days", this.getRetentionDays(), other.getRetentionDays(), sb);
            }
            if (!Objects.equal(getRetentionScanThrottleRate(), other.getRetentionScanThrottleRate())) {
                addToDiff("Retention scan throttle rate", this.getRetentionScanThrottleRate(), other.getRetentionScanThrottleRate(), sb);
            }
            if (!Objects.equal(getSerializerFactory() != null ? getSerializerFactory() : null,
                    other.getSerializerFactory() != null ? other.getSerializerFactory(): null)) {
                addToDiff("Serialization Factory", this.getSerializerFactory(), other.getSerializerFactory(), sb);
            }
            if (!Objects.equal(getHintedHandoffStrategyType(), other.getHintedHandoffStrategyType())) {
                addToDiff("Hinted handoff strategy", this.getHintedHandoffStrategyType().toDisplay(), other.getHintedHandoffStrategyType().toDisplay(), sb);
            }
            if (!Objects.equal(getHintPrefListSize(), other.getHintPrefListSize())) {
                addToDiff("Hinted preference list size", this.getHintPrefListSize(), other.getHintPrefListSize(), sb);
            }
            if (!Objects.equal(getMemoryFootprintMB(), other.getMemoryFootprintMB())) {
                addToDiff("Memory footprint (MB)", this.getMemoryFootprintMB(), other.getMemoryFootprintMB(), sb);
            }
            return sb.toString();
        }
    }

    private void addToDiff(String propertyName, long thisValue, long otherValue, StringBuilder sb) {
        addToDiff(propertyName, Long.toString(thisValue), Long.toString(otherValue), sb);
    }

    private void addToDiff(String propertyName, int thisValue, int otherValue, StringBuilder sb) {
        addToDiff(propertyName, Integer.toString(thisValue), Integer.toString(otherValue), sb);
    }

    private void addToDiff(String propertyName, String thisValue, String otherValue, StringBuilder sb) {
        sb.append(propertyName);
        sb.append(" differs; this: ");
        sb.append(thisValue);
        sb.append(" , other: ");
        sb.append(otherValue);
        sb.append("\n");
    }
}
