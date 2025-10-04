package click.dailyfeed.activity.domain.member.activity.redis;

import click.dailyfeed.code.domain.activity.transport.MemberActivityTransportDto;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@RequiredArgsConstructor
@Service
public class MemberActivityEventRedisService {

    @Value("${infrastructure.redis.event-queue.post-activity-event.list-key}")
    private String redisKey;

    @Value("${infrastructure.redis.event-queue.post-activity-event.dead-letter-list-key}")
    private String deadLetterKey;

    @Value("${infrastructure.redis.event-queue.post-activity-event.batch-size}")
    private Integer batchSize;

    @Qualifier("memberActivityTransportDtoRedisTemplate")
    private final RedisTemplate<String, MemberActivityTransportDto.MemberActivityEvent> redisTemplate;

    public void rPushEvent(MemberActivityTransportDto.MemberActivityEvent memberActivityEvent) {
        redisTemplate.opsForList().rightPush(redisKey, memberActivityEvent);
    }

    public List<MemberActivityTransportDto.MemberActivityEvent> lPopList() {
        List<MemberActivityTransportDto.MemberActivityEvent> result = redisTemplate.opsForList().leftPop(redisKey, batchSize);
        return result != null? result : List.of();
    }

    public void rPushDeadLetterEvent(List<MemberActivityTransportDto.MemberActivityEvent> postActivityEvent) {
        redisTemplate.opsForList().rightPushAll(deadLetterKey, postActivityEvent);
    }

    public List<MemberActivityTransportDto.MemberActivityEvent> lPopDeadLetterList() {
        List<MemberActivityTransportDto.MemberActivityEvent> result = redisTemplate.opsForList().rightPop(deadLetterKey, batchSize);
        return result != null? result : List.of();
    }
}
