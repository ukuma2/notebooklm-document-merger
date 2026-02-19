from datetime import datetime

from merger_engine import EmailThreader


def test_group_emails_handles_mixed_date_types_without_crashing():
    threader = EmailThreader()
    email_data = [
        {
            "subject": "Re: Release Plan",
            "date": "Mon, 1 Jan 2024 10:00:00 +0000",
            "file_path": "a.eml",
        },
        {
            "subject": "Release Plan",
            "date": None,
            "file_path": "b.eml",
        },
        {
            "subject": "FWD: Release Plan",
            "date": datetime(2023, 12, 1, 9, 0, 0),
            "file_path": "c.eml",
        },
        {
            "subject": "Release Plan",
            "date": "not-a-date",
            "file_path": "d.eml",
        },
    ]

    threads = threader.group_emails(email_data)
    assert "release plan" in threads

    ordered_files = [email["file_path"] for email in threads["release plan"]]

    # Invalid or missing dates are normalized to datetime.min and sorted first.
    assert ordered_files[0:2] == ["b.eml", "d.eml"]
    assert ordered_files[2:] == ["c.eml", "a.eml"]
