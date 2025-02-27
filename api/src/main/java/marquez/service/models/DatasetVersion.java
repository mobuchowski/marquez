/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package marquez.service.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import marquez.common.models.DatasetId;
import marquez.common.models.DatasetName;
import marquez.common.models.DatasetType;
import marquez.common.models.Field;
import marquez.common.models.NamespaceName;
import marquez.common.models.SourceName;
import marquez.common.models.TagName;
import marquez.common.models.Version;

@EqualsAndHashCode
@ToString
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = DbTableVersion.class, name = "DB_TABLE"),
  @JsonSubTypes.Type(value = StreamVersion.class, name = "STREAM")
})
public abstract class DatasetVersion {
  @Getter private final DatasetId id;
  @Getter private final DatasetType type;
  @Getter private final DatasetName name;
  @Getter private final DatasetName physicalName;
  @Getter private final Instant createdAt;
  @Getter private final Version version;
  @Getter private final NamespaceName namespace;
  @Getter private final SourceName sourceName;
  @Getter private final ImmutableList<Field> fields;
  @Getter private final ImmutableSet<TagName> tags;
  @Nullable private final String description;
  @Nullable @Setter private Run createdByRun;
  @Nullable @Setter private UUID createdByRunUuid;

  public DatasetVersion(
      @NonNull final DatasetId id,
      @NonNull final DatasetType type,
      @NonNull final DatasetName name,
      @NonNull final DatasetName physicalName,
      @NonNull final Instant createdAt,
      @NonNull final Version version,
      @NonNull final SourceName sourceName,
      @Nullable final ImmutableList<Field> fields,
      @Nullable final ImmutableSet<TagName> tags,
      @Nullable final String description,
      @Nullable final Run createdByRun) {
    this.id = id;
    this.type = type;
    this.name = name;
    this.physicalName = physicalName;
    this.createdAt = createdAt;
    this.version = version;
    this.namespace = id.getNamespace();
    this.sourceName = sourceName;
    this.fields = (fields == null) ? ImmutableList.of() : fields;
    this.tags = (tags == null) ? ImmutableSet.of() : tags;
    this.description = description;
    this.createdByRun = createdByRun;
  }

  public Optional<String> getDescription() {
    return Optional.ofNullable(description);
  }

  public Optional<Run> getCreatedByRun() {
    return Optional.ofNullable(createdByRun);
  }

  @JsonIgnore
  public UUID getCreatedByRunUuid() {
    return createdByRunUuid;
  }
}
