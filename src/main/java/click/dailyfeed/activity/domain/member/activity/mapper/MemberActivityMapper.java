package click.dailyfeed.activity.domain.member.activity.mapper;

import click.dailyfeed.activity.domain.member.activity.document.MemberActivityDocument;
import click.dailyfeed.code.domain.activity.exception.UndefinedMemberActivityEventTypeException;
import click.dailyfeed.code.domain.activity.type.MemberActivityType;
import click.dailyfeed.code.domain.activity.transport.MemberActivityTransportDto;
import org.springframework.stereotype.Component;

@Component
public class MemberActivityMapper {
    public MemberActivityDocument fromEvent(MemberActivityTransportDto.MemberActivityMessage message) {
        MemberActivityTransportDto.MemberActivityEvent event = message.getEvent();
        MemberActivityType memberActivityType = event.getMemberActivityType();
        if (MemberActivityType.postEventTypes.contains(memberActivityType)) {
            return MemberActivityDocument.ofNewPostActivity(
                    event.getMemberId(),
                    event.getPostId(),
                    event.getMemberActivityType()
            );

        } else if (MemberActivityType.commentEventTypes.contains(memberActivityType)) {
            return MemberActivityDocument.ofNewCommentActivity(
                    event.getMemberId(),
                    event.getPostId(),
                    event.getCommentId(),
                    event.getMemberActivityType()
            );
        } else if (MemberActivityType.memberEventTypes.contains(memberActivityType)) {
            return MemberActivityDocument.ofNewMemberActivity(
                    event.getMemberId(),
                    memberActivityType
            );
        } else if (MemberActivityType.postLikeEventTypes.contains(memberActivityType)) {
            return MemberActivityDocument.ofNewPostLikeActivity(
                    event.getMemberId(),
                    event.getPostId(),
                    memberActivityType
            );
        } else if (MemberActivityType.commentLikeEventTypes.contains(memberActivityType)) {
            return MemberActivityDocument.ofNewCommentLikeActivity(
                    event.getMemberId(),
                    event.getCommentId(),
                    memberActivityType
            );
        }

        throw new UndefinedMemberActivityEventTypeException();
    }
}
