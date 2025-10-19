package click.dailyfeed.activity.domain.deadletters.repository.mongo;

import click.dailyfeed.activity.domain.deadletters.document.ListenerDeadLetterDocument;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface ListenerDeadLetterRepository extends MongoRepository<ListenerDeadLetterDocument, ObjectId> {
    List<ListenerDeadLetterDocument> findByMessageKey(String messageKey);
}
