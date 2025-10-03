package click.dailyfeed.activity.domain.member.activity.repository.mongo;

import click.dailyfeed.activity.domain.member.activity.document.MemberActivityDocument;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface MemberActivityMongoRepository extends MongoRepository<MemberActivityDocument, ObjectId> {

}
