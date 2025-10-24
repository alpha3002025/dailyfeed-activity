package click.dailyfeed.activity.domain.member.activity.consumer;

import click.dailyfeed.activity.domain.member.activity.document.MemberActivityDocument;
import click.dailyfeed.activity.domain.member.activity.mapper.MemberActivityMapper;
import click.dailyfeed.activity.domain.member.activity.repository.mongo.MemberActivityMongoRepository;
import click.dailyfeed.activity.domain.member.activity.repository.mongo.MemberActivityMongoTemplate;
import click.dailyfeed.code.domain.activity.factory.MemberActivityTransferDtoFactory;
import click.dailyfeed.code.domain.activity.transport.MemberActivityTransportDto;
import click.dailyfeed.code.domain.activity.type.MemberActivityType;
import click.dailyfeed.code.global.kafka.exception.KafkaNetworkErrorException;
import click.dailyfeed.code.global.kafka.type.DateBasedTopicType;
import click.dailyfeed.code.global.redis.RedisKeyExistPredicate;
import click.dailyfeed.deadletter.domain.deadletter.document.KafkaListenerDeadLetterDocument;
import click.dailyfeed.deadletter.domain.deadletter.repository.mongo.KafkaListenerDeadLetterMongoTemplate;
import click.dailyfeed.deadletter.domain.deadletter.repository.mongo.KafkaListenerDeadLetterRepository;
import click.dailyfeed.kafka.domain.activity.redis.KafkaMessageKeyMemberActivityRedisService;
import click.dailyfeed.redis.global.deadletter.kafka.MemberActivityEventRedisService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
@Component
@Transactional
public class MemberActivityEventConsumer {
    private final MemberActivityEventRedisService memberActivityEventRedisService;
    private final KafkaMessageKeyMemberActivityRedisService kafkaMessageKeyMemberActivityRedisService;

    private final MemberActivityMongoRepository memberActivityMongoRepository;
    private final KafkaListenerDeadLetterRepository kafkaListenerDeadLetterRepository;
    private final MemberActivityMongoTemplate memberActivityMongoTemplate;
    private final KafkaListenerDeadLetterMongoTemplate kafkaListenerDeadLetterMongoTemplate;

    private final MemberActivityMapper memberActivityMapper;
    private final ObjectMapper objectMapper;

    private final Duration KAFKA_LISTENER_TTL = Duration.ofSeconds(30);

    @KafkaListener(
            topicPattern = DateBasedTopicType.MEMBER_ACTIVITY_PATTERN,
            groupId = "member-activity-consumer-group-activity-svc",
            containerFactory = "memberActivityKafkaListenerContainerFactory"
    )
    public void consumeAllMemberActivityEvents(
            @Payload MemberActivityTransportDto.MemberActivityEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String messageKey,
            Acknowledgment acknowledgment) {

        // 오프셋 및 이벤트 정보 로깅
        log.debug("📨 Consuming message - Topic: {}, Partition: {}, Offset: {}, MessageKey: {}, PostId: {}, EventType: {}",
                  topic, partition, offset, messageKey, event.getPostId(), event.getMemberActivityType());

        // Exactly Once 를 Off 해두었기에 중복메시지 수신 가능, 중복메시지 여부 체크
        if (RedisKeyExistPredicate.EXIST.equals(kafkaMessageKeyMemberActivityRedisService.checkExist(messageKey))) {
            return;
        }

        try {
            // 토픽명에서 날짜 추출
            String dateStr = DateBasedTopicType.MEMBER_ACTIVITY.extractDateFromTopicName(topic);

            // 이벤트 처리 (대기열에 저장)
            if (dateStr != null && dateStr.matches("\\d{8}")) { // 날짜 타입 처리
                MemberActivityTransportDto.MemberActivityMessage message = MemberActivityTransportDto.MemberActivityMessage.builder()
                        .key(messageKey)
                        .event(event)
                        .build();

                processEventByDate(message, dateStr);
                // 메시지 처리 성공 후 오프셋 커밋
                acknowledgment.acknowledge();

                kafkaMessageKeyMemberActivityRedisService.addAndExpireIn(message.getKey(), KAFKA_LISTENER_TTL);
                log.debug("✅ Offset committed - Topic: {}, Partition: {}, Offset: {}", topic, partition, offset);
            }


        } catch (Exception e) {
            log.error("❌ Failed to process message - Topic: {}, Partition: {}, Offset: {}, Error: {}",
                     topic, partition, offset, e.getMessage(), e);
            handleListenException(MemberActivityTransportDto.MemberActivityMessage.builder().key(messageKey).event(event).build());
        }
    }

    /**
     * 날짜별 이벤트 처리
     */
    private void processEventByDate(MemberActivityTransportDto.MemberActivityMessage message, String dateStr) {
        LocalDate eventDate = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyyMMdd"));
        LocalDate today = LocalDate.now();

        if (eventDate.equals(today)) {
            insertEvent(message);
        } else if (eventDate.isBefore(today)) { // 시차로 인해 오늘에서 내일로 넘어가는 케이스
            if(eventDate.isAfter(today.minusDays(2))) {
                insertEvent(message);
            }
            else{
                // 접미사가 yyyyMMdd 형식이 아닌 다른 형식의 토픽일 경우 이곳에서 처리 (운영을 위한 특정 용도)
            }
        } else {
            log.info("미래에서 오셨군요, 10년 뒤에 삼성전자 얼마에요?");
        }
    }

    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public void insertEvent(MemberActivityTransportDto.MemberActivityMessage message){
        final String messageKey = message.getKey();
        final MemberActivityTransportDto.MemberActivityEvent event = message.getEvent();
        if(!kafkaListenerDeadLetterRepository.findByMessageKey(messageKey).isEmpty()) return; // 이미 데드레터에 담은 메시지
        try{
            MemberActivityDocument document = memberActivityMapper.fromMessage(message);
            memberActivityMongoTemplate.upsertMemberActivity(document);
        } catch (Exception e) {
            DateTimeFormatter dateTimeFormatter = MemberActivityTransferDtoFactory.DATE_TIME_FORMATTER;
            String toRestore = String.format("messageKey=%s$$$memberId=%s$$$postId=%s$$$commentId=%s$$$activityType=%s$$$createdAt=%s$$$updatedAt%s$$$",
                    String.valueOf(messageKey), String.valueOf(event.getMemberId()), String.valueOf(event.getPostId()), String.valueOf(event.getCommentId()), event.getMemberActivityType().name(),
                    event.getCreatedAt().format(dateTimeFormatter), event.getUpdatedAt().format(dateTimeFormatter));
            try{ // deadletter 저장소에 저장
                // object mapper 직렬화
                String payload = objectMapper.writeValueAsString(message);

                // deadletter 저장소에 저장 시도
                try {
                    MemberActivityType.Category category = MemberActivityType.resolveCategory(event.getMemberActivityType());
                    KafkaListenerDeadLetterDocument document = KafkaListenerDeadLetterDocument.newDeadLetter(messageKey, payload, category);
                    kafkaListenerDeadLetterMongoTemplate.upsertKafkaListenerDeadLetter(document);
                } catch (Exception e1){ // deadletter 저장소에 저장 실패할 경우 PVC 로깅 기능을 활용
                    log.error(toRestore); // TODO :: 특정 PVC 에 로깅하는 기능으로 대체 필요
                }
            } catch (Exception e2){ // object mapper 를 이용해 객체 직렬화에 실패할 경우 (카프카 메시지 버전이 달라지는 케이스)
                log.error(toRestore); // TODO :: 특정 PVC 에 로깅하는 기능으로 대체 필요
            }
        }
    }

    /// scheduled 기반
    private void processLazy(MemberActivityTransportDto.MemberActivityMessage message) {
        // 1) Message read
        if (message == null) {
            return;
        }
        // 2) cache put
        memberActivityEventRedisService.rPushEvent(message);
        // 3) caching 완료된 메시지 키 저장
        kafkaMessageKeyMemberActivityRedisService.addAndExpireIn(message.getKey(), KAFKA_LISTENER_TTL);
    }

    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    @Scheduled(fixedRate = 1000, initialDelay = 5000)
    public void insertMany(){
        // 1초에 한번씩 동작 (애플리케이션 시작 5초 후부터)
        int processedBatches = 0;

        while(true){
            List<MemberActivityTransportDto.MemberActivityMessage> eventList = memberActivityEventRedisService.lPopList();
            if(eventList == null || eventList.isEmpty()){
                break;
            }

            try{
                log.info("📦 Processing batch #{} - size: {}", ++processedBatches, eventList.size());
                List<MemberActivityDocument> list = eventList
                        .stream()
                        .map(memberActivityMapper::fromMessage)
                        .toList();

                // MongoDB 저장
                memberActivityMongoRepository.saveAll(list);
                log.debug("✅ Successfully saved {} events to MongoDB", eventList.size());
            } catch (Exception e){
                log.error("❌ Failed to save batch to MongoDB", e);

                // kafka DLQ publish
                eventList.stream().forEach(message -> {
                    handleListenException(message);
                });

                log.info("📮 Moved {} events to dead letter queue", eventList.size());
            }
        }
    }

    public void handleListenException(MemberActivityTransportDto.MemberActivityMessage message){
        final String messageKey = message.getKey();
        if(!kafkaListenerDeadLetterRepository.findByMessageKey(messageKey).isEmpty()) return; // 이미 데드레터에 담은 메시지

        MemberActivityTransportDto.MemberActivityEvent event = message.getEvent();
        try{
            String payload = objectMapper.writeValueAsString(message);
            try {
                MemberActivityType.Category category = MemberActivityType.resolveCategory(event.getMemberActivityType());
                KafkaListenerDeadLetterDocument document = KafkaListenerDeadLetterDocument.newDeadLetter(messageKey, payload, category);
                kafkaListenerDeadLetterMongoTemplate.upsertKafkaListenerDeadLetter(document);
            } catch (Exception e){
                log.error("");
            }
        }
        catch (JsonProcessingException e){
            // 통신 메시지 형식(v1, v2, ...) 이 다른 것으로 인한 장애는 후보정 서비스에서 실행되도록하고 넘어간다. 애플리케이션 레벨에서 해결 불가능, 운영레벨에서 개발자가 대응
            // 여기까지 실패할 경우 'member-activity-yyyyMMdd 토픽을 전일자 토픽으로 돌면서 후보정 서비스에서 처리하도록 익셉션을 내고 그대로 둔다.
            throw new KafkaNetworkErrorException();
        }
    }
}
