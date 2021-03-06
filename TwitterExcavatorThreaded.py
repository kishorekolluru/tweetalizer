import json
import threading
from http.client import IncompleteRead
from io import BufferedWriter
from json import JSONEncoder
import tweepy
import _datetime

from pip._vendor.requests.packages.urllib3.exceptions import ProtocolError

twitter_auth = tweepy.OAuthHandler("Eug8CUA36W1QSxQiARfVR9jAQ", "p24M9jwx9BGLKCi5yxGbIKzBG4sCtmVpO5RzNzmQiCa60dd0KR")
twitter_auth.set_access_token("847427862021812224-TPX5Tyq0EsLIkKU3nDyhSzsALejM6bv",
                              "WPkWhmY2E8Fyf71DJOih4KNkvQjuX1HvlaFtxxyt5l0K9")

twitter_api = tweepy.API(twitter_auth)

# remove hastags, mentions, RT, unicode chars, links, stop words

positiveTrackList = [
    u'\U0000263A',
    u'\U0001F600',
    u'\U0001F601',
    u'\U0001F602',
    u'\U0001F603',
    u'\U0001F604',
    u'\U0001F605',
    u'\U0001F606',
    u'\U0001F607',
    u'\U0001F608',  # devilish smile
    u'\U0001F609',
    u'\U0001F60A',
    u'\U0001F60B',
    u'\U0001F60C',
    u'\U0001F60D',
    u'\U0001F60E',
    u'\U0001F60F',  # smirky face
    u'\U0001F923',  # rofl
    u'\U0001F92D',
    u'\U0001F92E',
    u'\U0001F913',  # geeky smile
    u'\U0001F920',  # cowboy hat face
    u'\U0001F921',  # clown
    u'\U0001F917',  # huggy face

    u'\U0001F642',
    u'\U0001F917',
    u'\U0001F60F',
    u'\U0001F61B',
    u'\U0001F61C',
    u'\U0001F61D',
    u'\U0001F643',  # upside down face
    u'\U0001F924',  # Drool
    u'\U0001F47B',  # Ghosty tongue out

    u'\U0001F4A9',  # Shit
    # Cat emojis
    u'\U0001F63A',
    u'\U0001F638',
    u'\U0001F639',
    u'\U0001F63B',
    u'\U0001F63C',
    u'\U0001F63D',
    # Monkey faces
    u'\U0001F648',
    u'\U0001F649',
    u'\U0001F64A',
    # Hand signs
    u'\U0001F44F',  # Applause
    u'\U0001F64C',  # hi fi
    u'\U0001F91D',  # Hand shake

    u'\U0001F48B',  # Lipstick lips
    u'\U0001F498',  # Cupid struck
    u'\U00002764',  # Heart
    u'\U0001F495',  # Hearts
    u'\U0001F496',  # Hearts
    u'\U0001F49C',  # Hearts
    u'\U0001F48C',  # Hearts
    u'\U0001F436',  # Pup
    u'\U0001F31E',  # Happy Sun
    u'\U0001F31D',  # Happy Moon
    u'\U00002B50',  # Star
    u'\U0001F383',  # Halloween pumpkin
    u'\U0001F389',  # Party snouts
    u'\U0001F38A',  # Party confetti
    u'\U0001F381',  # Gift
    u'\U0001F3C6',  # Winner Cup

]

positive_wordlist1 = [
    'a+',
    'abound',
    'abounds',
    'abundance',
    'abundant',
    'accessable',
    'accessible',
    'acclaim',
    'acclaimed',
    'acclamation',
    'accolade',
    'accolades',
    'accommodative',
    'accomodative',
    'accomplish',
    'accomplished',
    'accomplishment',
    'accomplishments',
    'accurate',
    'accurately',
    'achievable',
    'achievement',
    'achievements',
    'achievible',
    'acumen',
    'adaptable',
    'adaptive',
    'adequate',
    'adjustable',
    'admirable',
    'admirably',
    'admiration',
    'admire',
    'admirer',
    'admiring',
    'admiringly',
    'adorable',
    'adore',
    'adored',
    'adorer',
    'adoring',
    'adoringly',
    'adroit',
    'adroitly',
    'adulate',
    'adulation',
    'adulatory',
    'advanced',
    'advantage',
    'advantageous',
    'advantageously',
    'advantages',
    'adventuresome',
    'adventurous',
    'advocate',
    'advocated',
    'advocates',
    'affability',
    'affable',
    'affably',
    'affectation',
    'affection',
    'affectionate',
    'affinity',
    'affirm',
    'affirmation',
    'affirmative',
    'affluence',
    'affluent',
    'afford',
    'affordable',
    'affordably',
    'afordable',
    'agile',
    'agilely',
    'agility',
    'agreeable',
    'agreeableness',
    'agreeably',
    'all-around',
    'alluring',
    'alluringly',
    'altruistic',
    'altruistically',
    'amaze',
    'amazed',
    'amazement',
    'amazes',
    'amazing',
    'amazingly',
    'ambitious',
    'ambitiously',
    'ameliorate',
    'amenable',
    'amenity',
    'amiability',
    'amiabily',
    'amiable',
    'amicability',
    'amicable',
    'amicably',
    'amity',
    'ample',
    'amply',
    'amuse',
    'amusing',
    'amusingly',
    'angel',
    'angelic',
    'apotheosis',
    'appeal',
    'appealing',
    'applaud',
    'appreciable',
    'appreciate',
    'appreciated',
    'appreciates',
    'appreciative',
    'appreciatively',
    'appropriate',
    'approval',
    'approve',
    'ardent',
    'ardently',
    'ardor',
    'articulate',
    'aspiration',
    'aspirations',
    'aspire',
    'assurance',
    'assurances',
    'assure',
    'assuredly',
    'assuring',
    'astonish',
    'astonished',
    'astonishing',
    'astonishingly',
    'astonishment',
    'astound',
    'astounded',
    'astounding',
    'astoundingly',
    'astutely',
    'attentive',
    'attraction',
    'attractive',
    'attractively',
    'attune',
    'audible',
    'audibly',
    'auspicious',
    'authentic',
    'authoritative',
    'autonomous',
    'available',
    'aver',
    'avid',
    'avidly',
    'award',
    'awarded',
    'awards',
    'awe',
    'awed',
    'awesome',
    'awesomely',
    'awesomeness',
    'awestruck',
    'awsome',
    'backbone',
    'balanced',
    'bargain',
    'beauteous',
    'beautiful',
    'beautifullly',
    'beautifully',
    'beautify',
    'beauty',
    'beckon',
    'beckoned',
    'beckoning',
    'beckons',
    'believable',
    'believeable',
    'beloved',
    'benefactor',
    'beneficent',
    'beneficial',
    'beneficially',
    'beneficiary',
    'benefit',
    'benefits',
    'benevolence',
    'benevolent',
    'benifits',
    'best',
    'best-known',
    'best-performing',
    'best-selling',
    'better',
    'better-known',
    'better-than-expected',
    'beutifully',
    'blameless',
    'bless',
    'blessing',
    'bliss',
    'blissful',
    'blissfully',
    'blithe',
    'blockbuster',
    'bloom',
    'blossom',
    'bolster',
    'bonny',
    'bonus',
    'bonuses',
    'boom',
    'booming',
    'boost',
    'boundless',
    'bountiful',
    'brainiest',
    'brainy',
    'brand-new',
    'brave',
    'bravery',
    'bravo',
    'breakthrough',
    'breakthroughs',
    'breathlessness',
    'breathtaking',
    'breathtakingly',
    'breeze',
    'bright',
    'brighten',
    'brighter',
    'brightest',
    'brilliance',
    'brilliances',
    'brilliant',
    'brilliantly',
    'brisk',
    'brotherly',
    'bullish',
    'buoyant',
    'cajole',
    'calm',
    'calming',
    'calmness',
    'capability',
    'capable',
    'capably',
    'captivate',
    'captivating',
    'carefree',
    'cashback',
    'cashbacks',
    'catchy',
    'celebrate',
    'celebrated',
    'celebration',
    'celebratory',
    'champ',
    'champion',
    'charisma',
    'charismatic',
    'charitable',
    'charm',
    'charming',
    'charmingly',
    'chaste',
    'cheaper',
    'cheapest',
    'cheer',
    'cheerful',
    'cheery',
    'cherish',
    'cherished',
    'cherub',
    'chic',
    'chivalrous',
    'chivalry',
    'civility',
    'civilize',
    'clarity',
    'classic',
    'classy',
    'clean',
    'cleaner',
    'cleanest',
    'cleanliness',
    'cleanly',
    'clear',
    'clear-cut',
    'cleared',
    'clearer',
    'clearly',
    'clears',
    'clever',
    'cleverly',
    'cohere',
    'coherence',
    'coherent',
    'cohesive',
    'colorful',
    'comely',
    'comfort',
    'comfortable',
    'comfortably',
    'comforting',
    'comfy',
    'commend',
    'commendable',
    'commendably',
    'commitment',
    'commodious',
    'compact',
    'compactly',
    'compassion',
    'compassionate',
    'compatible',
    'competitive',
    'complement',
    'complementary',
    'complemented',
    'complements',
    'compliant',
    'compliment',
    'complimentary',
    'comprehensive',
    'conciliate',
    'conciliatory',
    'concise',
    'confidence',
    'confident',
    'congenial',
    'congratulate',
    'congratulation',
    'congratulations',
    'congratulatory',
    'conscientious',
    'considerate',
    'consistent',
    'consistently',
    'constructive',
    'consummate',
    'contentment',
    'continuity',
    'contrasty',
    'contribution',
    'convenience',
    'convenient',
    'conveniently',
    'convience',
    'convienient',
    'convient',
    'convincing',
    'convincingly',
    'cool',
    'coolest',
    'cooperative',
    'cooperatively',
    'cornerstone',
    'correct',
    'correctly',
    'cost-effective',
    'cost-saving',
    'counter-attack',
    'counter-attacks',
    'courage',
    'courageous',
    'courageously',
    'courageousness',
    'courteous',
    'courtly',
    'covenant',
    'cozy',
    'creative',
    'credence',
    'credible',
    'crisp',
    'crisper',
    'cure',
    'cure-all',
    'cushy',
    'cute',
    'cuteness',
    'danke',
    'danken',
    'daring',
    'daringly',
    'darling',
    'dashing',
    'dauntless',
    'dawn',
    'dazzle',
    'dazzled',
    'dazzling',
    'dead-cheap'
]

positive_wordlist2 = [

    'dead-on',
    'decency',
    'decent',
    'decisive',
    'decisiveness',
    'dedicated',
    'defeat',
    'defeated',
    'defeating',
    'defeats',
    'defender',
    'deference',
    'deft',
    'deginified',
    'delectable',
    'delicacy',
    'delicate',
    'delicious',
    'delight',
    'delighted',
    'delightful',
    'delightfully',
    'delightfulness',
    'dependable',
    'dependably',
    'deservedly',
    'deserving',
    'desirable',
    'desiring',
    'desirous',
    'destiny',
    'detachable',
    'devout',
    'dexterous',
    'dexterously',
    'dextrous',
    'dignified',
    'dignify',
    'dignity',
    'diligence',
    'diligent',
    'diligently',
    'diplomatic',
    'dirt-cheap',
    'distinction',
    'distinctive',
    'distinguished',
    'diversified',
    'divine',
    'divinely',
    'dominate',
    'dominated',
    'dominates',
    'dote',
    'dotingly',
    'doubtless',
    'dreamland',
    'dumbfounded',
    'dumbfounding',
    'dummy-proof',
    'durable',
    'dynamic',
    'eager',
    'eagerly',
    'eagerness',
    'earnest',
    'earnestly',
    'earnestness',
    'ease',
    'eased',
    'eases',
    'easier',
    'easiest',
    'easiness',
    'easing',
    'easy',
    'easy-to-use',
    'easygoing',
    'ebullience',
    'ebullient',
    'ebulliently',
    'ecenomical',
    'economical',
    'ecstasies',
    'ecstasy',
    'ecstatic',
    'ecstatically',
    'edify',
    'educated',
    'effective',
    'effectively',
    'effectiveness',
    'effectual',
    'efficacious',
    'efficient',
    'efficiently',
    'effortless',
    'effortlessly',
    'effusion',
    'effusive',
    'effusively',
    'effusiveness',
    'elan',
    'elate',
    'elated',
    'elatedly',
    'elation',
    'electrify',
    'elegance',
    'elegant',
    'elegantly',
    'elevate',
    'elite',
    'eloquence',
    'eloquent',
    'eloquently',
    'embolden',
    'eminence',
    'eminent',
    'empathize',
    'empathy',
    'empower',
    'empowerment',
    'enchant',
    'enchanted',
    'enchanting',
    'enchantingly',
    'encourage',
    'encouragement',
    'encouraging',
    'encouragingly',
    'endear',
    'endearing',
    'endorse',
    'endorsed',
    'endorsement',
    'endorses',
    'endorsing',
    'energetic',
    'energize',
    'energy-efficient',
    'energy-saving',
    'engaging',
    'engrossing',
    'enhance',
    'enhanced',
    'enhancement',
    'enhances',
    'enjoy',
    'enjoyable',
    'enjoyably',
    'enjoyed',
    'enjoying',
    'enjoyment',
    'enjoys',
    'enlighten',
    'enlightenment',
    'enliven',
    'ennoble',
    'enough',
    'enrapt',
    'enrapture',
    'enraptured',
    'enrich',
    'enrichment',
    'enterprising',
    'entertain',
    'entertaining',
    'entertains',
    'enthral',
    'enthrall',
    'enthralled',
    'enthuse',
    'enthusiasm',
    'enthusiast',
    'enthusiastic',
    'enthusiastically',
    'entice',
    'enticed',
    'enticing',
    'enticingly',
    'entranced',
    'entrancing',
    'entrust',
    'enviable',
    'enviably',
    'envious',
    'enviously',
    'enviousness',
    'envy',
    'equitable',
    'ergonomical',
    'err-free',
    'erudite',
    'ethical',
    'eulogize',
    'euphoria',
    'euphoric',
    'euphorically',
    'evaluative',
    'evenly',
    'eventful',
    'everlasting',
    'evocative',
    'exalt',
    'exaltation',
    'exalted',
    'exaltedly',
    'exalting',
    'exaltingly',
    'examplar',
    'examplary',
    'excallent',
    'exceed',
    'exceeded',
    'exceeding',
    'exceedingly',
    'exceeds',
    'excel',
    'exceled',
    'excelent',
    'excellant',
    'excelled',
    'excellence',
    'excellency',
    'excellent',
    'excellently',
    'excels',
    'exceptional',
    'exceptionally',
    'excite',
    'excited',
    'excitedly',
    'excitedness',
    'excitement',
    'excites',
    'exciting',
    'excitingly',
    'exellent',
    'exemplar',
    'exemplary',
    'exhilarate',
    'exhilarating',
    'exhilaratingly',
    'exhilaration',
    'exonerate',
    'expansive',
    'expeditiously',
    'expertly',
    'exquisite',
    'exquisitely',
    'extol',
    'extoll',
    'extraordinarily',
    'extraordinary',
    'exuberance',
    'exuberant',
    'exuberantly',
    'exult',
    'exultant',
    'exultation',
    'exultingly',
    'eye-catch',
    'eye-catching',
    'eyecatch',
    'eyecatching',
    'fabulous',
    'fabulously',
    'facilitate',
    'fair',
    'fairly',
    'fairness',
    'faith',
    'faithful',
    'faithfully',
    'faithfulness',
    'fame',
    'famed',
    'famous',
    'famously',
    'fancier',
    'fancinating',
    'fancy',
    'fanfare',
    'fans',
    'fantastic',
    'fantastically',
    'fascinate',
    'fascinating',
    'fascinatingly',
    'fascination',
    'fashionable',
    'fashionably',
    'fast',
    'fast-growing',
    'fast-paced',
    'faster',
    'fastest',
    'fastest-growing',
    'faultless',
    'fav',
    'fave',
    'favor',
    'favorable',
    'favored',
    'favorite',
    'favorited',
    'favour',
    'fearless',
    'fearlessly',
    'feasible',
    'feasibly',
    'feat',
    'feature-rich',
    'fecilitous',
    'feisty',
    'felicitate',
    'felicitous',
    'felicity',
    'fertile',
    'fervent',
    'fervently',
    'fervid',
    'fervidly',
    'fervor',
    'festive',
    'fidelity',
    'fiery',
    'fine',
    'fine-looking',
    'finely',
    'finer',
    'finest',
    'firmer',
    'first-class',
    'first-in-class',
    'first-rate',
    'flashy',
    'flatter',
    'flattering',
    'flatteringly',
    'flawless',
    'flawlessly',
    'flexibility',
    'flexible',
    'flourish',
    'flourishing',
    'fluent',
    'flutter',
    'fond',
    'fondly',
    'fondness',
    'foolproof',
    'foremost',
    'foresight',
    'formidable',
    'fortitude',
    'fortuitous',
    'fortuitously',
    'fortunate',
    'fortunately',
    'fortune',
    'fragrant',
    'free',
    'freed',
    'freedom',
    'freedoms',
    'fresh',
    'fresher',
    'freshest',
    'friendliness',
    'friendly',
    'frolic',
    'frugal',
    'fruitful',
    'ftw',
    'fulfillment',
    'fun',
    'futurestic',
    'futuristic',
    'gaiety',
    'gaily',
    'gain',
    'gained',
    'gainful',
    'gainfully',
    'gaining',
    'gains',
    'gallant',
    'gallantly',
    'galore',
    'geekier',
    'geeky',
    'gem',
    'gems',
    'generosity',
    'generous',
    'generously',
    'genial',
    'genius'
]

positive_wordlist3 = [

    'gentle',
    'gentlest',
    'genuine',
    'gifted',
    'glad',
    'gladden',
    'gladly',
    'gladness',
    'glamorous',
    'glee',
    'gleeful',
    'gleefully',
    'glimmer',
    'glimmering',
    'glisten',
    'glistening',
    'glitter',
    'glitz',
    'glorify',
    'glorious',
    'gloriously',
    'glory',
    'glow',
    'glowing',
    'glowingly',
    'god-given',
    'god-send',
    'godlike',
    'godsend',
    'gold',
    'golden',
    'good',
    'goodly',
    'goodness',
    'goodwill',
    'goood',
    'gooood',
    'gorgeous',
    'gorgeously',
    'grace',
    'graceful',
    'gracefully',
    'gracious',
    'graciously',
    'graciousness',
    'grand',
    'grandeur',
    'grateful',
    'gratefully',
    'gratification',
    'gratified',
    'gratifies',
    'gratify',
    'gratifying',
    'gratifyingly',
    'gratitude',
    'great',
    'greatest',
    'greatness',
    'grin',
    'groundbreaking',
    'guarantee',
    'guidance',
    'guiltless',
    'gumption',
    'gush',
    'gusto',
    'gutsy',
    'hail',
    'halcyon',
    'hale',
    'hallmark',
    'hallmarks',
    'hallowed',
    'handier',
    'handily',
    'hands-down',
    'handsome',
    'handsomely',
    'handy',
    'happier',
    'happily',
    'happiness',
    'happy',
    'hard-working',
    'hardier',
    'hardy',
    'harmless',
    'harmonious',
    'harmoniously',
    'harmonize',
    'harmony',
    'headway',
    'heal',
    'healthful',
    'healthy',
    'hearten',
    'heartening',
    'heartfelt',
    'heartily',
    'heartwarming',
    'heaven',
    'heavenly',
    'helped',
    'helpful',
    'helping',
    'hero',
    'heroic',
    'heroically',
    'heroine',
    'heroize',
    'heros',
    'high-quality',
    'high-spirited',
    'hilarious',
    'holy',
    'homage',
    'honest',
    'honesty',
    'honor',
    'honorable',
    'honored',
    'honoring',
    'hooray',
    'hopeful',
    'hospitable',
    'hot',
    'hotcake',
    'hotcakes',
    'hottest',
    'hug',
    'humane',
    'humble',
    'humility',
    'humor',
    'humorous',
    'humorously',
    'humour',
    'humourous',
    'ideal',
    'idealize',
    'ideally',
    'idol',
    'idolize',
    'idolized',
    'idyllic',
    'illuminate',
    'illuminati',
    'illuminating',
    'illumine',
    'illustrious',
    'ilu',
    'imaculate',
    'imaginative',
    'immaculate',
    'immaculately',
    'immense',
    'impartial',
    'impartiality',
    'impartially',
    'impassioned',
    'impeccable',
    'impeccably',
    'important',
    'impress',
    'impressed',
    'impresses',
    'impressive',
    'impressively',
    'impressiveness',
    'improve',
    'improved',
    'improvement',
    'improvements',
    'improves',
    'improving',
    'incredible',
    'incredibly',
    'indebted',
    'individualized',
    'indulgence',
    'indulgent',
    'industrious',
    'inestimable',
    'inestimably',
    'inexpensive',
    'infallibility',
    'infallible',
    'infallibly',
    'influential',
    'ingenious',
    'ingeniously',
    'ingenuity',
    'ingenuous',
    'ingenuously',
    'innocuous',
    'innovation',
    'innovative',
    'inpressed',
    'insightful',
    'insightfully',
    'inspiration',
    'inspirational',
    'inspire',
    'inspiring',
    'instantly',
    'instructive',
    'instrumental',
    'integral',
    'integrated',
    'intelligence',
    'intelligent',
    'intelligible',
    'interesting',
    'interests',
    'intimacy',
    'intimate',
    'intricate',
    'intrigue',
    'intriguing',
    'intriguingly',
    'intuitive',
    'invaluable',
    'invaluablely',
    'inventive',
    'invigorate',
    'invigorating',
    'invincibility',
    'invincible',
    'inviolable',
    'inviolate',
    'invulnerable',
    'irreplaceable',
    'irreproachable',
    'irresistible',
    'irresistibly',
    'issue-free',
    'jaw-droping',
    'jaw-dropping',
    'jollify',
    'jolly',
    'jovial',
    'joy',
    'joyful',
    'joyfully',
    'joyous',
    'joyously',
    'jubilant',
    'jubilantly',
    'jubilate',
    'jubilation',
    'jubiliant',
    'judicious',
    'justly',
    'keen',
    'keenly',
    'keenness',
    'kid-friendly',
    'kindliness',
    'kindly',
    'kindness',
    'knowledgeable',
    'kudos',
    'large-capacity',
    'laud',
    'laudable',
    'laudably',
    'lavish',
    'lavishly',
    'law-abiding',
    'lawful',
    'lawfully',
    'lead',
    'leading',
    'leads',
    'lean',
    'led',
    'legendary',
    'leverage',
    'levity',
    'liberate',
    'liberation',
    'liberty',
    'lifesaver',
    'light-hearted',
    'lighter',
    'likable',
    'like',
    'liked',
    'likes',
    'liking',
    'lionhearted',
    'lively',
    'logical',
    'long-lasting',
    'lovable',
    'lovably',
    'love',
    'loved',
    'loveliness',
    'lovely',
    'lover',
    'loves',
    'loving',
    'low-cost',
    'low-price',
    'low-priced',
    'low-risk',
    'lower-priced',
    'loyal',
    'loyalty',
    'lucid',
    'lucidly',
    'luck',
    'luckier',
    'luckiest',
    'luckiness',
    'lucky',
    'lucrative',
    'luminous',
    'lush',
    'luster',
    'lustrous',
    'luxuriant',
    'luxuriate',
    'luxurious',
    'luxuriously',
    'luxury',
    'lyrical',
    'magic',
    'magical',
    'magnanimous',
    'magnanimously',
    'magnificence',
    'magnificent',
    'magnificently',
    'majestic',
    'majesty',
    'manageable',
    'maneuverable',
    'marvel',
    'marveled',
    'marvelled',
    'marvellous',
    'marvelous',
    'marvelously',
    'marvelousness',
    'marvels',
    'master',
    'masterful',
    'masterfully',
    'masterpiece',
    'masterpieces',
    'masters',
    'mastery',
    'matchless',
    'mature',
    'maturely',
    'maturity',
    'meaningful',
    'memorable',
    'merciful',
    'mercifully',
    'mercy',
    'merit',
    'meritorious',
    'merrily',
    'merriment',
    'merriness',
    'merry',
    'mesmerize',
    'mesmerized',
    'mesmerizes',
    'mesmerizing',
    'mesmerizingly',
    'meticulous',
    'meticulously',
    'mightily',
    'mighty',
    'mind-blowing',
    'miracle',
    'miracles',
    'miraculous',
    'miraculously',
    'miraculousness',
    'modern',
    'modest',
    'modesty',
    'momentous',
    'monumental',
    'monumentally',
    'morality',
    'motivated',
    'multi-purpose',
    'navigable',
    'neat',
    'neatest',
    'neatly',
    'nice',
    'nicely'
]

positive_wordlist4 = [

    'nicer',
    'nicest',
    'nifty',
    'nimble',
    'noble',
    'nobly',
    'noiseless',
    'non-violence',
    'non-violent',
    'notably',
    'noteworthy',
    'nourish',
    'nourishing',
    'nourishment',
    'novelty',
    'nurturing',
    'oasis',
    'obsession',
    'obsessions',
    'obtainable',
    'openly',
    'openness',
    'optimal',
    'optimism',
    'optimistic',
    'opulent',
    'orderly',
    'originality',
    'outdo',
    'outdone',
    'outperform',
    'outperformed',
    'outperforming',
    'outperforms',
    'outshine',
    'outshone',
    'outsmart',
    'outstanding',
    'outstandingly',
    'outstrip',
    'outwit',
    'ovation',
    'overjoyed',
    'overtake',
    'overtaken',
    'overtakes',
    'overtaking',
    'overtook',
    'overture',
    'pain-free',
    'painless',
    'painlessly',
    'palatial',
    'pamper',
    'pampered',
    'pamperedly',
    'pamperedness',
    'pampers',
    'panoramic',
    'paradise',
    'paramount',
    'pardon',
    'passion',
    'passionate',
    'passionately',
    'patience',
    'patient',
    'patiently',
    'patriot',
    'patriotic',
    'peace',
    'peaceable',
    'peaceful',
    'peacefully',
    'peacekeepers',
    'peach',
    'peerless',
    'pep',
    'pepped',
    'pepping',
    'peppy',
    'peps',
    'perfect',
    'perfection',
    'perfectly',
    'permissible',
    'perseverance',
    'persevere',
    'personages',
    'personalized',
    'phenomenal',
    'phenomenally',
    'picturesque',
    'piety',
    'pinnacle',
    'playful',
    'playfully',
    'pleasant',
    'pleasantly',
    'pleased',
    'pleases',
    'pleasing',
    'pleasingly',
    'pleasurable',
    'pleasurably',
    'pleasure',
    'plentiful',
    'pluses',
    'plush',
    'plusses',
    'poetic',
    'poeticize',
    'poignant',
    'poise',
    'poised',
    'polished',
    'polite',
    'politeness',
    'popular',
    'portable',
    'posh',
    'positive',
    'positively',
    'positives',
    'powerful',
    'powerfully',
    'praise',
    'praiseworthy',
    'praising',
    'pre-eminent',
    'precious',
    'precise',
    'precisely',
    'preeminent',
    'prefer',
    'preferable',
    'preferably',
    'prefered',
    'preferes',
    'preferring',
    'prefers',
    'premier',
    'prestige',
    'prestigious',
    'prettily',
    'pretty',
    'priceless',
    'pride',
    'principled',
    'privilege',
    'privileged',
    'prize',
    'proactive',
    'problem-free',
    'problem-solver',
    'prodigious',
    'prodigiously',
    'prodigy',
    'productive',
    'productively',
    'proficient',
    'proficiently',
    'profound',
    'profoundly',
    'profuse',
    'profusion',
    'progress',
    'progressive',
    'prolific',
    'prominence',
    'prominent',
    'promise',
    'promised',
    'promises',
    'promising',
    'promoter',
    'prompt',
    'promptly',
    'proper',
    'properly',
    'propitious',
    'propitiously',
    'pros',
    'prosper',
    'prosperity',
    'prosperous',
    'prospros',
    'protect',
    'protection',
    'protective',
    'proud',
    'proven',
    'proves',
    'providence',
    'proving',
    'prowess',
    'prudence',
    'prudent',
    'prudently',
    'punctual',
    'pure',
    'purify',
    'purposeful',
    'quaint',
    'qualified',
    'qualify',
    'quicker',
    'quiet',
    'quieter',
    'radiance',
    'radiant',
    'rapid',
    'rapport',
    'rapt',
    'rapture',
    'raptureous',
    'raptureously',
    'rapturous',
    'rapturously',
    'rational',
    'razor-sharp',
    'reachable',
    'readable',
    'readily',
    'ready',
    'reaffirm',
    'reaffirmation',
    'realistic',
    'realizable',
    'reasonable',
    'reasonably',
    'reasoned',
    'reassurance',
    'reassure',
    'receptive',
    'reclaim',
    'recomend',
    'recommend',
    'recommendation',
    'recommendations',
    'recommended',
    'reconcile',
    'reconciliation',
    'record-setting',
    'recover',
    'recovery',
    'rectification',
    'rectify',
    'rectifying',
    'redeem',
    'redeeming',
    'redemption',
    'refine',
    'refined',
    'refinement',
    'reform',
    'reformed',
    'reforming',
    'reforms',
    'refresh',
    'refreshed',
    'refreshing',
    'refund',
    'refunded',
    'regal',
    'regally',
    'regard',
    'rejoice',
    'rejoicing',
    'rejoicingly',
    'rejuvenate',
    'rejuvenated',
    'rejuvenating',
    'relaxed',
    'relent',
    'reliable',
    'reliably',
    'relief',
    'relish',
    'remarkable',
    'remarkably',
    'remedy',
    'remission',
    'remunerate',
    'renaissance',
    'renewed',
    'renown',
    'renowned',
    'replaceable',
    'reputable',
    'reputation',
    'resilient',
    'resolute',
    'resound',
    'resounding',
    'resourceful',
    'resourcefulness',
    'respect',
    'respectable',
    'respectful',
    'respectfully',
    'respite',
    'resplendent',
    'responsibly',
    'responsive',
    'restful',
    'restored',
    'restructure',
    'restructured',
    'restructuring',
    'retractable',
    'revel',
    'revelation',
    'revere',
    'reverence',
    'reverent',
    'reverently',
    'revitalize',
    'revival',
    'revive',
    'revives',
    'revolutionary',
    'revolutionize',
    'revolutionized',
    'revolutionizes',
    'reward',
    'rewarding',
    'rewardingly',
    'rich',
    'richer',
    'richly',
    'richness',
    'right',
    'righten',
    'righteous',
    'righteously',
    'righteousness',
    'rightful',
    'rightfully',
    'rightly',
    'rightness',
    'risk-free',
    'robust',
    'rock-star',
    'rock-stars',
    'rockstar',
    'rockstars',
    'romantic',
    'romantically',
    'romanticize',
    'roomier',
    'roomy',
    'rosy',
    'safe',
    'safely',
    'sagacity',
    'sagely',
    'saint',
    'saintliness',
    'saintly',
    'salutary',
    'salute',
    'sane',
    'satisfactorily',
    'satisfactory',
    'satisfied',
    'satisfies',
    'satisfy',
    'satisfying',
    'satisified',
    'saver',
    'savings',
    'savior',
    'savvy',
    'scenic',
    'seamless',
    'seasoned',
    'secure',
    'securely',
    'selective',
    'self-determination',
    'self-respect',
    'self-satisfaction',
    'self-sufficiency',
    'self-sufficient',
    'sensation',
    'sensational',
    'sensationally',
    'sensations',
    'sensible',
    'sensibly',
    'sensitive',
    'serene',
    'serenity',
    'sexy',
    'sharp',
    'sharper',
    'sharpest',
    'shimmering',
    'shimmeringly'
]

positive_wordlist5 = [

    'shine',
    'shiny',
    'significant',
    'silent',
    'simpler',
    'simplest',
    'simplified',
    'simplifies',
    'simplify',
    'simplifying',
    'sincere',
    'sincerely',
    'sincerity',
    'skill',
    'skilled',
    'skillful',
    'skillfully',
    'slammin',
    'sleek',
    'slick',
    'smart',
    'smarter',
    'smartest',
    'smartly',
    'smile',
    'smiles',
    'smiling',
    'smilingly',
    'smitten',
    'smooth',
    'smoother',
    'smoothes',
    'smoothest',
    'smoothly',
    'snappy',
    'snazzy',
    'sociable',
    'soft',
    'softer',
    'solace',
    'solicitous',
    'solicitously',
    'solid',
    'solidarity',
    'soothe',
    'soothingly',
    'sophisticated',
    'soulful',
    'soundly',
    'soundness',
    'spacious',
    'sparkle',
    'sparkling',
    'spectacular',
    'spectacularly',
    'speedily',
    'speedy',
    'spellbind',
    'spellbinding',
    'spellbindingly',
    'spellbound',
    'spirited',
    'spiritual',
    'splendid',
    'splendidly',
    'splendor',
    'spontaneous',
    'sporty',
    'spotless',
    'sprightly',
    'stability',
    'stabilize',
    'stable',
    'stainless',
    'standout',
    'state-of-the-art',
    'stately',
    'statuesque',
    'staunch',
    'staunchly',
    'staunchness',
    'steadfast',
    'steadfastly',
    'steadfastness',
    'steadiest',
    'steadiness',
    'steady',
    'stellar',
    'stellarly',
    'stimulate',
    'stimulates',
    'stimulating',
    'stimulative',
    'stirringly',
    'straighten',
    'straightforward',
    'streamlined',
    'striking',
    'strikingly',
    'striving',
    'strong',
    'stronger',
    'strongest',
    'stunned',
    'stunning',
    'stunningly',
    'stupendous',
    'stupendously',
    'sturdier',
    'sturdy',
    'stylish',
    'stylishly',
    'stylized',
    'suave',
    'suavely',
    'sublime',
    'subsidize',
    'subsidized',
    'subsidizes',
    'subsidizing',
    'substantive',
    'succeed',
    'succeeded',
    'succeeding',
    'succeeds',
    'succes',
    'success',
    'successes',
    'successful',
    'successfully',
    'suffice',
    'sufficed',
    'suffices',
    'sufficient',
    'sufficiently',
    'suitable',
    'sumptuous',
    'sumptuously',
    'sumptuousness',
    'super',
    'superb',
    'superbly',
    'superior',
    'superiority',
    'supple',
    'support',
    'supported',
    'supporter',
    'supporting',
    'supportive',
    'supports',
    'supremacy',
    'supreme',
    'supremely',
    'supurb',
    'supurbly',
    'surmount',
    'surpass',
    'surreal',
    'survival',
    'survivor',
    'sustainability',
    'sustainable',
    'swank',
    'swankier',
    'swankiest',
    'swanky',
    'sweeping',
    'sweet',
    'sweeten',
    'sweetheart',
    'sweetly',
    'sweetness',
    'swift',
    'swiftness',
    'talent',
    'talented',
    'talents',
    'tantalize',
    'tantalizing',
    'tantalizingly',
    'tempt',
    'tempting',
    'temptingly',
    'tenacious',
    'tenaciously',
    'tenacity',
    'tender',
    'tenderly',
    'terrific',
    'terrifically',
    'thank',
    'thankful',
    'thinner',
    'thoughtful',
    'thoughtfully',
    'thoughtfulness',
    'thrift',
    'thrifty',
    'thrill',
    'thrilled',
    'thrilling',
    'thrillingly',
    'thrills',
    'thrive',
    'thriving',
    'thumb-up',
    'thumbs-up',
    'tickle',
    'tidy',
    'time-honored',
    'timely',
    'tingle',
    'titillate',
    'titillating',
    'titillatingly',
    'togetherness',
    'tolerable',
    'toll-free',
    'top',
    'top-notch',
    'top-quality',
    'topnotch',
    'tops',
    'tough',
    'tougher',
    'toughest',
    'traction',
    'tranquil',
    'tranquility',
    'transparent',
    'treasure',
    'tremendously',
    'trendy',
    'triumph',
    'triumphal',
    'triumphant',
    'triumphantly',
    'trivially',
    'trophy',
    'trouble-free',
    'trump',
    'trumpet',
    'trust',
    'trusted',
    'trusting',
    'trustingly',
    'trustworthiness',
    'trustworthy',
    'trusty',
    'truthful',
    'truthfully',
    'truthfulness',
    'twinkly',
    'ultra-crisp',
    'unabashed',
    'unabashedly',
    'unaffected',
    'unassailable',
    'unbeatable',
    'unbiased',
    'unbound',
    'uncomplicated',
    'unconditional',
    'undamaged',
    'undaunted',
    'understandable',
    'undisputable',
    'undisputably',
    'undisputed',
    'unencumbered',
    'unequivocal',
    'unequivocally',
    'unfazed',
    'unfettered',
    'unforgettable',
    'unity',
    'unlimited',
    'unmatched',
    'unparalleled',
    'unquestionable',
    'unquestionably',
    'unreal',
    'unrestricted',
    'unrivaled',
    'unselfish',
    'unwavering',
    'upbeat',
    'upgradable',
    'upgradeable',
    'upgraded',
    'upheld',
    'uphold',
    'uplift',
    'uplifting',
    'upliftingly',
    'upliftment',
    'upscale',
    'usable',
    'useable',
    'useful',
    'user-friendly',
    'user-replaceable',
    'valiant',
    'valiantly',
    'valor',
    'valuable',
    'variety',
    'venerate',
    'verifiable',
    'veritable',
    'versatile',
    'versatility',
    'vibrant',
    'vibrantly',
    'victorious',
    'victory',
    'viewable',
    'vigilance',
    'vigilant',
    'virtue',
    'virtuous',
    'virtuously',
    'visionary',
    'vivacious',
    'vivid',
    'vouch',
    'vouchsafe',
    'warm',
    'warmer',
    'warmhearted',
    'warmly',
    'warmth',
    'wealthy',
    'welcome',
    'well',
    'well-backlit',
    'well-balanced',
    'well-behaved',
    'well-being',
    'well-bred',
    'well-connected',
    'well-educated',
    'well-established',
    'well-informed',
    'well-intentioned',
    'well-known',
    'well-made',
    'well-managed',
    'well-mannered',
    'well-positioned',
    'well-received',
    'well-regarded',
    'well-rounded',
    'well-run',
    'well-wishers',
    'wellbeing',
    'whoa',
    'wholeheartedly',
    'wholesome',
    'whooa',
    'whoooa',
    'wieldy',
    'willing',
    'willingly',
    'willingness',
    'win',
    'windfall',
    'winnable',
    'winner',
    'winners',
    'winning',
    'wins',
    'wisdom',
    'wise',
    'wisely',
    'witty',
    'won',
    'wonder',
    'wonderful',
    'wonderfully',
    'wonderous',
    'wonderously',
    'wonders',
    'wondrous',
    'woo',
    'work',
    'workable',
    'worked',
    'works',
    'world-famous',
    'worth',
    'worth-while',
    'worthiness',
    'worthwhile',
    'worthy',
    'wow',
    'wowed',
    'wowing',
    'wows'
]

positive_wordlist6 = [

    'yay',
    'youthful',
    'zeal',
    'zenith',
    'zest',
    'zippy'
]


class TwitterInfo:
    def __init__(self, created_at, id_str, text, source, user_location, user_id, user_name, user_lang,
                 geo, coordinates, place, hashtags, lang, filter_level, timestamp_ms):
        self.created_at = created_at
        self.id_str = id_str
        self.text = text
        self.source = source
        self.user_location = user_location
        self.user_id = user_id
        self.user_name = user_name
        self.user_lang = user_lang
        self.geo = geo
        self.coordinates = coordinates
        self.place = place
        self.hashtags = hashtags
        self.lang = lang
        self.filter_level = filter_level
        self.timestamp_ms = timestamp_ms


class MyStreamListener(tweepy.StreamListener):
    def __init__(self, file_handle):
        self.file_handle = file_handle
        self.counter = 0
        print('{\n"twitterdata" : [', file=file_handle, end='\n')

    def on_status(self, status):
        # print("######### New Tweet########: {}".format(status.text))
        pass

    def on_data(self, raw_data):
        self.counter += 1
        print("Thread {} :: Tweet {} received".format(threading.current_thread().getName(), self.counter))
        json_data = json.loads(raw_data)
        #        print(raw_data)
        if 'created_at' in json_data:
            printed_json = json.dumps(TwitterInfo(json_data["created_at"],
                                                  json_data["id_str"],
                                                  json_data["text"],
                                                  json_data["source"],
                                                  json_data["user"]['location'],
                                                  json_data["user"]['id_str'],
                                                  json_data["user"]['name'],
                                                  json_data["user"]['lang'],
                                                  json_data["geo"],
                                                  json_data["coordinates"],
                                                  json_data["place"],
                                                  json_data["entities"]["hashtags"],
                                                  json_data["lang"],
                                                  json_data["filter_level"],
                                                  json_data["timestamp_ms"]).__dict__)
            print(printed_json + '\n,', file=self.file_handle, end='\n')

    def on_disconnect(self, notice):
        print("Disconnecting...{}".format(notice))

    def on_error(self, status_code):
        if status_code == 420:
            print("Error code 420! Stopping streaming!!!!!!!")
            return False
        elif status_code == 123:
            print("You interrupted!!")
            return False


class TwitterThread(threading.Thread):
    def __init__(self, outfile, wordlist):
        super(TwitterThread, self).__init__()
        self.outfile = outfile
        self.word_list = wordlist
        self.my_stream_listener = MyStreamListener(self.outfile)
        self.my_stream = tweepy.Stream(auth=twitter_api.auth, listener=self.my_stream_listener)

    def run(self):
        print("Runnning {}".format(self.getName()))
        self.my_stream.filter(track=self.word_list, async=True, languages=['en'])

    def disconnect(self):
        self.my_stream.disconnect()
        print('"end": "true"\n]\n}', file=self.outfile, end='\n')
        self.outfile.close()
        print("Thread {} Stopped".format(self.getName()))


def main():
    buffer_size = 50000
    
# ADJUST FILE NAMES AFTER EACH RUN
    file_name_thread1 = 'raw_tweets_wordpos_thread5.json'
    # file_name_thread2 = 'raw_tweets_wordpos_thread2.json'

    outfile1 = open(file_name_thread1, mode='w', buffering=buffer_size)
    # outfile2 = open(file_name_thread2, mode='w', buffering=buffer_size)


# UPDATE THE WORD LIST AFTER EACH PAIR OF LISTS IS DONE
    thread1 = TwitterThread(outfile1, positive_wordlist5)
    # thread2 = TwitterThread(outfile2, positive_wordlist2)

    thread1.start()
    # thread2.start()

# SAME 'S' FOR STOP WORKS HERE
    cmd = 'a'
    while cmd != 's':
        cmd = input("COMMAND: ")

    thread1.disconnect()
    # thread2.disconnect()


if __name__ == "__main__": main()
