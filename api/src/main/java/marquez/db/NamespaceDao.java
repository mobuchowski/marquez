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

package marquez.db;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import marquez.db.mappers.NamespaceMapper;
import marquez.db.mappers.NamespaceRowMapper;
import marquez.db.mappers.OwnerRowMapper;
import marquez.db.models.NamespaceRow;
import marquez.db.models.OwnerRow;
import marquez.service.models.Namespace;
import marquez.service.models.NamespaceMeta;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.transaction.Transaction;

@RegisterRowMapper(NamespaceRowMapper.class)
@RegisterRowMapper(NamespaceMapper.class)
@RegisterRowMapper(OwnerRowMapper.class)
public interface NamespaceDao extends BaseDao {

  @Transaction
  default Namespace upsert(String namespaceName, NamespaceMeta meta) {
    Instant now = Instant.now();
    NamespaceRow namespaceRow;
    if (meta.getDescription().isPresent()) {
      namespaceRow =
          upsert(
              UUID.randomUUID(),
              now,
              namespaceName,
              meta.getOwnerName().getValue(),
              meta.getDescription().get());
    } else {
      namespaceRow = upsert(UUID.randomUUID(), now, namespaceName, meta.getOwnerName().getValue());
    }
    if (namespaceRow.getCurrentOwnerName() != null
        && !namespaceRow.getCurrentOwnerName().equalsIgnoreCase(meta.getOwnerName().getValue())) {
      OwnerRow oldOwner = findOwner(namespaceRow.getCurrentOwnerName());
      OwnerRow ownerRow = upsertOwner(UUID.randomUUID(), now, meta.getOwnerName().getValue());
      if (oldOwner != null) {
        endOwnership(now, namespaceRow.getUuid(), oldOwner.getUuid());
      }
      startOwnership(UUID.randomUUID(), now, namespaceRow.getUuid(), ownerRow.getUuid());
      setCurrentOwner(namespaceRow.getUuid(), now, ownerRow.getName());
    }
    return findBy(namespaceRow.getName()).get();
  }

  @SqlQuery("SELECT EXISTS (SELECT 1 FROM namespaces WHERE name = :name)")
  boolean exists(String name);

  @SqlUpdate(
      "UPDATE namespaces "
          + "SET updated_at = :updatedAt, "
          + "    current_owner_name = :currentOwnerName "
          + "WHERE uuid = :rowUuid")
  void setCurrentOwner(UUID rowUuid, Instant updatedAt, String currentOwnerName);

  @SqlQuery("SELECT * FROM namespaces WHERE name = :name")
  Optional<Namespace> findBy(String name);

  @SqlQuery("SELECT * FROM namespaces WHERE name = :name")
  Optional<NamespaceRow> findByRow(String name);

  @SqlQuery("SELECT * FROM namespaces ORDER BY name LIMIT :limit OFFSET :offset")
  List<Namespace> findAll(int limit, int offset);

  @SqlQuery(
      "INSERT INTO namespaces ( "
          + "uuid, "
          + "created_at, "
          + "updated_at, "
          + "name, "
          + "current_owner_name "
          + ") VALUES ("
          + ":uuid, "
          + ":now, "
          + ":now, "
          + ":name, "
          + ":currentOwnerName "
          + ") ON CONFLICT(name) DO "
          + "UPDATE SET "
          + "updated_at = EXCLUDED.updated_at "
          + "RETURNING *")
  NamespaceRow upsert(UUID uuid, Instant now, String name, String currentOwnerName);

  @SqlQuery(
      "INSERT INTO namespaces ( "
          + "uuid, "
          + "created_at, "
          + "updated_at, "
          + "name, "
          + "current_owner_name, "
          + "description "
          + ") VALUES ("
          + ":uuid, "
          + ":now, "
          + ":now, "
          + ":name, "
          + ":currentOwnerName, "
          + ":description "
          + ") ON CONFLICT(name) DO "
          + "UPDATE SET "
          + "updated_at = EXCLUDED.updated_at "
          + "RETURNING *")
  NamespaceRow upsert(
      UUID uuid, Instant now, String name, String currentOwnerName, String description);

  @SqlQuery(
      "INSERT INTO owners (uuid, created_at, name) "
          + "VALUES ( "
          + ":uuid, :now, :name "
          + ") ON CONFLICT(name) DO "
          + "UPDATE SET "
          + "created_at = EXCLUDED.created_at "
          + "RETURNING *")
  OwnerRow upsertOwner(UUID uuid, Instant now, String name);

  @SqlQuery("SELECT * FROM owners WHERE name = :name")
  OwnerRow findOwner(String name);

  @SqlUpdate(
      "INSERT INTO namespace_ownerships (uuid, started_at, namespace_uuid, owner_uuid) "
          + "VALUES (:uuid, :startedAt, :namespaceUuid, :ownerUuid)")
  void startOwnership(UUID uuid, Instant startedAt, UUID namespaceUuid, UUID ownerUuid);

  @SqlUpdate(
      "UPDATE namespace_ownerships "
          + "SET ended_at = :endedAt "
          + "WHERE namespace_uuid = :namespaceUuid AND owner_uuid = :ownerUuid")
  void endOwnership(Instant endedAt, UUID namespaceUuid, UUID ownerUuid);
}
