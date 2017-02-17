"""
Utility methods related to course
"""
import logging
from django.conf import settings

from opaque_keys.edx.keys import CourseKey
from openedx.core.djangoapps.content.course_overviews.models import CourseOverview

log = logging.getLogger(__name__)


def get_lms_link_for_about_page(course_key):
    """
    Arguments:
        course_key: A CourseKey object identifying the course.

    Returns the course sharing url, this can be one of course's social sharing url, marketing url, or
    lms course about url.
    """
    assert isinstance(course_key, CourseKey)

    course_overview = CourseOverview.objects.get(id=course_key)
    if course_overview.social_sharing_url:
        course_about_url = course_overview.social_sharing_url
    elif course_overview.marketing_url:
        course_about_url = course_overview.marketing_url
    else:
        course_about_url = u"{about_base_url}/courses/{course_key}/about".format(
            about_base_url=settings.LMS_ROOT_URL,
            course_key=unicode(course_key),
        )

    return course_about_url
