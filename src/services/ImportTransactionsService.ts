import Transaction from '../models/Transaction';
import {In, getCustomRepository, getRepository} from 'typeorm'
import csvParse from 'csv-parse'
import fs from 'fs'
import TransactionsRepository from '../repositories/TransactionsRepository';
import Category from '../models/Category';

interface CSVTransaction{
  title: string;
  type: 'income' | 'outcome',
  value: number;
  category: string;
}

class ImportTransactionsService {
  async execute(filePath: string): Promise<Transaction[]> {
    const transactionRepository = getCustomRepository(TransactionsRepository)
    const categoriesRepository = getRepository(Category)
    const contactsReadStream = fs.createReadStream(filePath)

    const parsers = csvParse({
      from_line: 2,
    })

    const transactions:CSVTransaction[] = []
    const categories:string[] = []

    const parseCSV = contactsReadStream.pipe(parsers)
    parseCSV.on('data', async line => {
      const [title, type, value, category] = line.map((cell: string) =>
      cell.trim()
      )
      if (!title || !type || !value) return;

       categories.push(category)
       transactions.push({title, type, value, category})
    })

    await new Promise(resolve => parseCSV.on('end', resolve))    

    const existentsCategories = await categoriesRepository.find({
      where: {
        title: In(categories)
      }
    })

    const existentCategoriesTitles = existentsCategories.map(
      (category: Category) => category.title
    )

    const addCategoryTitles = categories.filter(
      category => !existentCategoriesTitles.includes(category)
    ).filter((value, index, self) => self.indexOf(value) == index)

    const newCategories = categoriesRepository.create(
      addCategoryTitles.map(title => ({
        title,
      })),
    )
  
    await categoriesRepository.save(newCategories)
    const finalCategories = [...newCategories, ...existentsCategories]

    const createdTransactions = transactionRepository.create(
      transactions.map(transaction => (
        {
          title: transaction.title,
          type: transaction.type,
          value: transaction.value,
          category: finalCategories.find(
            category => category.title
          ),
        })),
    )

    await transactionRepository.save(createdTransactions)
    await fs.promises.unlink(filePath)

    return createdTransactions
  }  
}

export default ImportTransactionsService;
