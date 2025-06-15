from sqlalchemy import create_engine, text
import os

db_url = os.getenv("DB_URL")
if not db_url:
    raise ValueError("DB_URL environment variable is not set!")

engine = create_engine(db_url)

def load_jobs_from_db():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM jobs"))
        jobs = [dict(row._mapping) for row in result]
    return jobs

def load_job_from_db(id):
    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM jobs WHERE id = :val"), {'val': id})
        rows = result.all()
        if len(rows) == 0:
            return None
        else:
            return dict(rows[0]._mapping)

def add_application_to_db(job_id, data):
    with engine.begin() as conn:  # Ensures commit on success
        query = text("""
            INSERT INTO applications (
                job_id, full_name, email, linkedin_url, education, work_experience, resume_url
            ) VALUES (
                :job_id, :full_name, :email, :linkedin_url, :education, :work_experience, :resume_url
            )
        """)
        conn.execute(query, {
            "job_id": job_id,
            "full_name": data['full_name'],
            "email": data['email'],
            "linkedin_url": data['linkedin_url'],
            "education": data['education'],
            "work_experience": data['work_experience'],
            "resume_url": data['resume_url']
        })

        