from score import *

def iter_grpd_submissions(submission_queue_id):
    grpd_submissions = defaultdict(lambda: defaultdict(list))
    for submission, status in syn.getSubmissionBundles(submission_queue_id):
        # skip unscored submissions
        if status['status'] != 'SCORED': continue
        principalId = submission.teamId if 'teamId' in submission else submission.userId
        creation_ts = parse(submission['createdOn'], fuzzy=True)
        file_handles = json.loads(submission['entityBundleJSON'])['fileHandles']
        assert len(file_handles) == 1
        submission_fname = file_handles[0]['fileName']
        if submission_fname == 'NOT_SET':
            print "Skipping: %s" % submission_fname
            continue
        factor, sample = submission_fname.split('.')[1:3]
        filename = "{}/{}.{}.{}".format(SUBMISSIONS_DIR, principalId, submission.id, submission_fname)
        if not os.path.isfile(filename): 
            print "Skipping: %s" % filename
            continue
        grpd_submissions[
            (factor, sample)][
                principalId].append((creation_ts, filename))
    
    for leader_board, factor_submissions in grpd_submissions.iteritems():
        yield leader_board, factor_submissions

    return

ScoreRecord = namedtuple(
    'ScoreRecord', 
    'factor sample principalId submission_date submission_fname bootstrap_index recall_at_10_fdr recall_at_50_fdr auPRC auROC rank'
)
def score_record_factory(cursor, row):
    row = list(row)
    row[3] = parse(row[3], fuzzy=True)
    return ScoreRecord(*row)

def calc_and_insert_new_results(
        DB, factor, sample, principalId, submission_date, submission_fname):
    # sort by submission date
    print (factor, sample), principalId, submission_date, submission_fname
    full_results, labels, scores = score_main(submission_fname)
    print "FULL", full_results
    all_res = []
    for bootstrap_i, results in calc_bootstrapped_scores(labels, scores):
        print bootstrap_i, results
        all_res.append([
            factor, 
            sample,

            principalId,
            submission_date,
            
            submission_fname,

            bootstrap_i,

            results.recall_at_10_fdr,
            results.recall_at_50_fdr,
            results.auPRC,
            results.auROC,
            
            -1
        ])
    
    while True:
        try:
            conn = sqlite3.connect(DB)
            c = conn.cursor()
            for res in all_res:
                c.execute(
                    "INSERT INTO scores VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
                    res
                )
            c.close()
            conn.commit()
            conn.close()
        except sqlite3.OperationalError:
            conn.close()
            time.sleep(1)
            continue
        else:
            break
    
    return

def estimate_bootstrapped_scores(DB, submission_queue_id):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute('''
    CREATE TABLE IF NOT EXISTS scores  (
        factor text,
        sample text,

        principalId int,
        submission_date text,
        submission_fname text,
        
        bootstrap_index int,

        recall_at_10_fdr real, 
        recall_at_50_fdr real, 
        auPRC real, 
        auROC real,
        
        rank int
    );''')
    c.close()
    conn.commit()
    conn.close()
    
    submission_args = []
    for (factor, sample), factor_submissions in iter_grpd_submissions(
            submission_queue_id):
        for principalId, submissions in factor_submissions.iteritems():
            submissions.sort(reverse=True)
            for (submission_index, 
                 (submission_date, submission_fname)) in enumerate(submissions):
                # skip old submissions
                if submission_index > 0: continue
                conn = sqlite3.connect(DB)
                c = conn.cursor()
                c.execute(
                    "SELECT * FROM scores WHERE factor=? AND sample=? AND principalId=? AND submission_date=?",
                    (
                        factor,
                        sample,
                        principalId,
                        submission_date
                    )
                )
                res = c.fetchall()
                c.close()
                conn.close()
                if len(res) == 0:
                    submission_args.append([
                        DB, 
                        factor, sample, 
                        principalId, submission_date, submission_fname
                    ])

    run_in_parallel(NTHREADS, calc_and_insert_new_results, submission_args)
    return

def calc_combined_ranks(records):
    # make sure that the principalId is unique
    principal_ids = [x.principalId for x in records]
    submission_ids = [x.submission_fname.split(".")[1] for x in records]
    attrs_to_rank = ['recall_at_10_fdr', 'recall_at_50_fdr', 'auPRC', 'auROC']
    scores = np.zeros(len(principal_ids), dtype=float)
    for user_i, attr in enumerate(attrs_to_rank):
        attr_scores = np.array([getattr(x, attr) for x in records])
        ranks = rankdata(-attr_scores, "average")
        pval_scores = np.log(ranks/float(len(ranks) + 1))
        scores += pval_scores
    ranks = rankdata(scores, "average")
    return dict(zip(zip(principal_ids, submission_ids), ranks))

def filter_older_submissions(submissions):
    """Choose the most recent submission for each user.   

    """
    filtered_submissions = {}
    for submission in submissions:
        if (submission.principalId not in filtered_submissions
            or (filtered_submissions[submission.principalId].submission_date 
                < submission.submission_date)
            ):
            filtered_submissions[submission.principalId] = submission
    return filtered_submissions.values()

def get_name(principalId):
    try: 
        res = syn.restGET('/team/{id}'.format(id=principalId))
        return res['name']
    except:
        profile = syn.getUserProfile(principalId)
        return profile['userName']

GlobalScore = namedtuple('GlobalScore', ['principalId', 'name', 'score_lb', 'score_mean', 'score_ub', 'rank'])
def calculate_ranks_from_DB(DB):
    conn = sqlite3.connect(DB)
    conn.row_factory = score_record_factory
    c = conn.cursor()
    c.execute("SELECT * FROM scores ORDER BY bootstrap_index, principalId;")
    sample_grpd_results = defaultdict(lambda: defaultdict(list))
    all_users = set()
    for x in c.fetchall():
        sample_key = (x.sample, x.factor)
        sample_grpd_results[(x.sample, x.factor)][x.bootstrap_index].append(x)
        all_users.add(x.principalId)
    
    # group all submissions by tf name and sample
    rv = {}
    global_scores = defaultdict(lambda: defaultdict(list))
    for (tf_name, sample), bootstrapped_submissions in sample_grpd_results.iteritems():
        user_ranks = defaultdict(list)
        for index, submissions in bootstrapped_submissions.iteritems():
            submissions = filter_older_submissions(submissions)
            ranks = calc_combined_ranks(submissions)
            obs_users = set(x[0] for x in ranks.keys())
            for (principalId, submission_id), rank in ranks.iteritems():
                user_ranks[(principalId, submission_id)].append(rank)
                global_scores[index][principalId].append(
                    min(0.5, rank/(len(ranks)+1))
                )
            for principalId in all_users - obs_users:
                global_scores[index][principalId].append(0.5)
        
        print tf_name, sample
        for (principalId, submission_id), ranks in sorted(
                user_ranks.iteritems(), key=lambda x: sorted(x[1])[1]):
            print principalId, get_name(principalId), submission_id, sorted(ranks)[1]
            assert submission_id not in rv
            rv[int(submission_id)] = sorted(ranks)[1]
        print

    # group the scores by user
    user_grpd_global_scores = defaultdict(list)
    user_grpd_global_ranks = defaultdict(list)
    for bootstrap_index, bootstrap_global_scores in global_scores.iteritems():
        sorted_scores = sorted(
            bootstrap_global_scores.iteritems(), key=lambda x: sum(x[1]))
        ranks = rankdata([sum(x[1]) for x in sorted_scores])
        for (principalId, scores), rank in zip(sorted_scores, ranks):
            user_grpd_global_scores[principalId].append(sum(scores)/float(len(scores)))
            user_grpd_global_ranks[principalId].append(rank)
    global_data = []
    for principalId, scores in sorted(
            user_grpd_global_scores.iteritems(), key=lambda x: sum(x[1])):
        global_data.append(GlobalScore(*[
            principalId, get_name(principalId), 
            min(scores), sum(scores)/len(scores), max(scores), 
            sorted(user_grpd_global_ranks[principalId])[1]
        ]))
    global_data = sorted(global_data, key=lambda x: (x.rank, x.score_mean))
    for x in global_data: print x
    return rv, global_data

def update_global_scores_table(global_data):
    import challenge_config as config
    from synapseclient import Schema, Column, Table, Row, RowSet, as_table_columns
    # 'principalId', 'name', 'score_lb', 'score_mean', 'score_ub', 'rank'
    cols = [
        Column(name='UserID', columnType='STRING', maximumSize=100),
        Column(name='Name', columnType='STRING', maximumSize=100),
        Column(name='score_lb', columnType='DOUBLE'),
        Column(name='score_mean', columnType='DOUBLE'),
        Column(name='score_ub', columnType='DOUBLE'),
        Column(name='rank', columnType='DOUBLE'),
    ]
    schema = Schema(name='Global Scores', columns=cols, parent=config.CHALLENGE_SYN_ID)

    results = syn.tableQuery("select * from {}".format('syn7237020'))
    if len(results) > 0:
        a = syn.delete(results.asRowSet())
    table = syn.store(Table(schema, global_data))
    results = syn.tableQuery("select * from {}".format(table.tableId))
    for row in results:
        print row
    return

def update_ranks(evaluation, dry_run=False):
    if type(evaluation) != Evaluation:
        evaluation = syn.getEvaluation(evaluation)

    ranks, global_data = calculate_ranks_from_DB(DB)
    for submission, status in syn.getSubmissionBundles(evaluation, status='SCORED'):
        submission_id = int(submission['id'])
        current_annotations = synapseclient.annotations.from_submission_status_annotations(
            status["annotations"])
        rank = ranks[submission_id] if submission_id in ranks else -1
        print submission_id, rank
        current_annotations['rank'] = rank
        status.annotations = synapseclient.annotations.to_submission_status_annotations(
            current_annotations, is_private=False)
        status = syn.store(status)

    # update the global data table
    update_global_scores_table(global_data)


def calc_bootstrapped_scores(labels, scores):
    from sklearn.cross_validation import StratifiedKFold
    for i, (indices, _) in enumerate(
            StratifiedKFold(labels, n_folds=10, random_state=0)):
        results = ClassificationResult(
            labels[indices], scores[indices].round(), scores[indices])
        yield i, results
    return

if __name__ == '__main__':
    SUBMISSION_QUEUE_ID = 7071644
    estimate_bootstrapped_scores(DB, SUBMISSION_QUEUE_ID)
    update_ranks(SUBMISSION_QUEUE_ID)
